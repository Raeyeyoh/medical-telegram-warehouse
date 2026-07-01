from __future__ import annotations

import sys
sys.path.append('../src')

import argparse
import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UsernameInvalidError,
)
from telethon.tl.types import MessageMediaPhoto
from utils.logger import get_logger


load_dotenv()

logger = get_logger(__name__, filename="scraper.log")

TG_API_ID = os.getenv("TG_API_ID")
TG_API_HASH = os.getenv("TG_API_HASH")
TG_PHONE = os.getenv("TG_PHONE")
TG_SESSION_NAME = os.getenv("TG_SESSION_NAME", "medical_scraper_session")
DEFAULT_CHANNELS = [
    c.strip() for c in os.getenv("TG_CHANNELS", "").split(",") if c.strip()
]
DATA_LAKE_ROOT = Path(os.getenv("DATA_LAKE_ROOT", "data/raw"))
MESSAGES_ROOT = DATA_LAKE_ROOT / "telegram_messages"
IMAGES_ROOT = DATA_LAKE_ROOT / "images"

DEFAULT_MESSAGE_LIMIT = 100


@dataclass
class ScrapeStats:
    """Simple per-channel bookkeeping so we can log a clean summary."""

    channel: str
    messages_fetched: int = 0
    images_downloaded: int = 0
    errors: list[str] = field(default_factory=list)


def _today_partition() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _ensure_dirs(*dirs: Path) -> None:
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


async def _download_image(client: TelegramClient, message, channel_name: str) -> str | None:
    """Download a message's photo, if any, to data/raw/images/{channel}/{message_id}.jpg.

    Returns the relative path stored in the data lake, or None if there was
    no photo / the download failed.
    """
    if not message.media or not isinstance(message.media, MessageMediaPhoto):
        return None

    channel_dir = IMAGES_ROOT / channel_name
    _ensure_dirs(channel_dir)
    target_path = channel_dir / f"{message.id}.jpg"

    try:
        await client.download_media(message.media, file=str(target_path))
        return str(target_path)
    except Exception as exc:
        logger.error(
            f"[{channel_name}] Failed to download image for message {message.id}: {exc}")
        return None


async def scrape_channel(
    client: TelegramClient, channel_name: str, limit: int
) -> tuple[list[dict], ScrapeStats]:
    """Scrape up to `limit` recent messages from a single channel.

    Returns the list of normalized message records (JSON-serializable) plus
    a ScrapeStats summary for logging.
    """
    stats = ScrapeStats(channel=channel_name)
    records: list[dict] = []

    logger.info(f"[{channel_name}] Starting scrape (limit={limit})")

    try:
        entity = await client.get_entity(channel_name)
    except (ChannelPrivateError, UsernameInvalidError, ValueError) as exc:
        msg = f"Could not resolve channel '{channel_name}': {exc}"
        logger.error(msg)
        stats.errors.append(msg)
        return records, stats

    try:
        async for message in client.iter_messages(entity, limit=limit):
            if message.action is not None and message.text is None and message.media is None:
                continue

            image_path = await _download_image(client, message, channel_name)
            if image_path:
                stats.images_downloaded += 1

            record = {
                "message_id": message.id,
                "channel_name": channel_name,
                "message_date": message.date.isoformat() if message.date else None,
                "message_text": message.text or "",
                "has_media": message.media is not None,
                "image_path": image_path,
                "views": getattr(message, "views", None),
                "forwards": getattr(message, "forwards", None),
                "raw": json.loads(message.to_json()),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
            records.append(record)
            stats.messages_fetched += 1

    except FloodWaitError as exc:
        msg = f"Rate limited on '{channel_name}', must wait {exc.seconds}s. Stopping this channel for now."
        logger.warning(msg)
        stats.errors.append(msg)
    except Exception as exc:
        msg = f"Unexpected error scraping '{channel_name}': {exc}"
        logger.error(msg)
        stats.errors.append(msg)

    logger.info(
        f"[{channel_name}] Done — {stats.messages_fetched} messages, "
        f"{stats.images_downloaded} images, {len(stats.errors)} errors"
    )
    return records, stats


def _write_partition(channel_name: str, records: list[dict]) -> Path:
    """Write records to data/raw/telegram_messages/YYYY-MM-DD/{channel}.json.

    If the file already exists for today (e.g. re-running the scraper),
    new records are appended and de-duplicated by message_id.
    """
    partition_dir = MESSAGES_ROOT / _today_partition()
    _ensure_dirs(partition_dir)
    out_path = partition_dir / f"{channel_name}.json"

    existing: list[dict] = []
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning(
                f"Existing file {out_path} was not valid JSON — overwriting.")

    combined_by_id = {r["message_id"]: r for r in existing}
    combined_by_id.update({r["message_id"]: r for r in records})
    combined = list(combined_by_id.values())

    out_path.write_text(json.dumps(combined, indent=2,
                        ensure_ascii=False), encoding="utf-8")
    return out_path


async def run(channels: list[str], limit: int) -> None:
    if not (TG_API_ID and TG_API_HASH and TG_PHONE):
        raise SystemExit(
            "Missing Telegram credentials. Set TG_API_ID, TG_API_HASH, TG_PHONE in .env "
            "(see .env.example) — get these from https://my.telegram.org"
        )
    if not channels:
        raise SystemExit(
            "No channels to scrape. Set TG_CHANNELS in .env or pass --channels.")

    _ensure_dirs(MESSAGES_ROOT, IMAGES_ROOT)

    client = TelegramClient(TG_SESSION_NAME, int(TG_API_ID), TG_API_HASH)
    await client.start(phone=TG_PHONE)
    logger.info(
        f"Telegram client authenticated. Scraping {len(channels)} channel(s): {channels}")

    run_summary: list[ScrapeStats] = []

    for channel_name in channels:
        records, stats = await scrape_channel(client, channel_name, limit)
        if records:
            out_path = _write_partition(channel_name, records)
            logger.info(
                f"[{channel_name}] Wrote {len(records)} records to {out_path}")
        run_summary.append(stats)

    await client.disconnect()

    total_messages = sum(s.messages_fetched for s in run_summary)
    total_images = sum(s.images_downloaded for s in run_summary)
    total_errors = sum(len(s.errors) for s in run_summary)
    logger.info(
        f"Scrape run complete: {total_messages} messages, {total_images} images, "
        f"{total_errors} errors across {len(run_summary)} channel(s)."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Telegram channels into the raw data lake.")
    parser.add_argument(
        "--channels",
        type=str,
        default=None,
        help="Comma-separated channel usernames. Defaults to TG_CHANNELS in .env.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_MESSAGE_LIMIT,
        help=f"Max messages to fetch per channel (default {DEFAULT_MESSAGE_LIMIT}).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    channel_list = (
        [c.strip() for c in args.channels.split(",") if c.strip()]
        if args.channels
        else DEFAULT_CHANNELS
    )
    asyncio.run(run(channel_list, args.limit))
