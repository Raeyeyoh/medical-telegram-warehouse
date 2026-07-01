
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import scraper  


@pytest.fixture(autouse=True)
def _isolate_data_lake(tmp_path, monkeypatch):
    """Redirect the module-level data lake paths to a temp dir for each test."""
    fake_root = tmp_path / "data" / "raw"
    monkeypatch.setattr(scraper, "DATA_LAKE_ROOT", fake_root)
    monkeypatch.setattr(scraper, "MESSAGES_ROOT",
                        fake_root / "telegram_messages")
    monkeypatch.setattr(scraper, "IMAGES_ROOT", fake_root / "images")
    yield


def _sample_record(message_id, text="hello"):
    return {
        "message_id": message_id,
        "channel_name": "test_channel",
        "message_date": "2026-06-30T12:00:00+00:00",
        "message_text": text,
        "has_media": False,
        "image_path": None,
        "views": 10,
        "forwards": 1,
        "raw": {},
        "scraped_at": "2026-07-01T00:00:00+00:00",
    }


def test_write_partition_creates_file_with_records():
    records = [_sample_record(1), _sample_record(2)]
    out_path = scraper._write_partition("test_channel", records)

    assert out_path.exists()
    saved = json.loads(out_path.read_text(encoding="utf-8"))
    assert len(saved) == 2
    assert {r["message_id"] for r in saved} == {1, 2}


def test_write_partition_dedups_by_message_id_on_rerun():
    first_run = [_sample_record(1, text="original")]
    scraper._write_partition("test_channel", first_run)

    second_run = [_sample_record(1, text="updated"),
                  _sample_record(2, text="new")]
    out_path = scraper._write_partition("test_channel", second_run)

    saved = json.loads(out_path.read_text(encoding="utf-8"))
    assert len(saved) == 2  
    by_id = {r["message_id"]: r for r in saved}
    assert by_id[1]["message_text"] == "updated"
    assert by_id[2]["message_text"] == "new"


def test_write_partition_handles_corrupt_existing_file(tmp_path):
    partition_dir = scraper.MESSAGES_ROOT / scraper._today_partition()
    partition_dir.mkdir(parents=True, exist_ok=True)
    corrupt_path = partition_dir / "test_channel.json"
    corrupt_path.write_text("{not valid json", encoding="utf-8")

    records = [_sample_record(5)]
    out_path = scraper._write_partition("test_channel", records)

    saved = json.loads(out_path.read_text(encoding="utf-8"))
    assert len(saved) == 1
    assert saved[0]["message_id"] == 5
