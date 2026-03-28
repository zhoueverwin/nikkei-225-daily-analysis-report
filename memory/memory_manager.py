"""Memory manager - handles structured storage and retrieval of daily analysis records."""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class MemoryManager:
    """Manages short/medium/long term memory for analysis continuity."""

    def __init__(self, store_dir: str = "memory/store", short_term_days: int = 7, medium_term_days: int = 30):
        self.store_dir = Path(store_dir)
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.short_term_days = short_term_days
        self.medium_term_days = medium_term_days

    def _file_path(self, date: str) -> Path:
        return self.store_dir / f"{date}.json"

    def save_daily_record(self, record: dict[str, Any]) -> None:
        """Save a daily analysis record."""
        date = record.get("date", datetime.now().strftime("%Y-%m-%d"))
        path = self._file_path(date)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved memory for {date}")

    def load_daily_record(self, date: str) -> dict[str, Any] | None:
        """Load a single day's record."""
        path = self._file_path(date)
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Failed to load memory for {date}: {e}")
            return None

    def load_recent(self, days: int | None = None) -> list[dict[str, Any]]:
        """Load recent N days of memory (default: short_term_days)."""
        if days is None:
            days = self.short_term_days
        records = []
        today = datetime.now()
        for i in range(days):
            date = (today - timedelta(days=i + 1)).strftime("%Y-%m-%d")
            record = self.load_daily_record(date)
            if record:
                records.append(record)
        return list(reversed(records))  # chronological order

    def get_latest_record(self) -> dict[str, Any] | None:
        """Get the most recent available record."""
        today = datetime.now()
        for i in range(self.short_term_days):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            record = self.load_daily_record(date)
            if record:
                return record
        return None

    def get_stance_history(self, days: int = 7) -> list[dict[str, Any]]:
        """Get stance changes over time for continuity tracking."""
        records = self.load_recent(days)
        return [
            {
                "date": r["date"],
                "stance": r.get("stance", {}),
                "core_conclusion": r.get("core_conclusion", ""),
            }
            for r in records
            if "stance" in r
        ]

    def get_open_observations(self) -> list[str]:
        """Get unresolved observations from recent records."""
        latest = self.get_latest_record()
        if latest:
            return latest.get("open_observations", [])
        return []

    def get_prediction_history(self, days: int = 30) -> list[dict[str, Any]]:
        """Get prediction records for accuracy tracking."""
        records = self.load_recent(days)
        predictions = []
        for r in records:
            for pred in r.get("predictions", []):
                predictions.append({
                    "date": r["date"],
                    **pred,
                })
        return predictions

    def create_daily_record(
        self,
        date: str,
        market_snapshot: dict,
        core_conclusion: str,
        stance: dict,
        predictions: list[dict],
        open_observations: list[str],
        sector_highlights: dict | None = None,
        sentiment_score: float | None = None,
        contradiction_flags: list[str] | None = None,
        prediction_verification: dict | None = None,
    ) -> dict[str, Any]:
        """Create a structured daily memory record."""
        record = {
            "date": date,
            "market_data_snapshot": market_snapshot,
            "core_conclusion": core_conclusion,
            "stance": stance,
            "predictions": predictions,
            "open_observations": open_observations,
            "sector_highlights": sector_highlights or {},
            "sentiment_score": sentiment_score,
            "contradiction_flags": contradiction_flags or [],
            "prediction_verification": prediction_verification,
        }
        return record

    def cleanup_old_records(self) -> int:
        """Compress records older than medium_term to summaries. Returns count of compressed records."""
        cutoff = datetime.now() - timedelta(days=self.medium_term_days)
        compressed = 0

        for path in sorted(self.store_dir.glob("*.json")):
            try:
                date_str = path.stem
                record_date = datetime.strptime(date_str, "%Y-%m-%d")
                if record_date < cutoff:
                    record = self.load_daily_record(date_str)
                    if record and "market_data_snapshot" in record:
                        # Keep only summary fields
                        summary = {
                            "date": record["date"],
                            "core_conclusion": record.get("core_conclusion"),
                            "stance": record.get("stance"),
                            "predictions": record.get("predictions"),
                            "prediction_verification": record.get("prediction_verification"),
                            "_compressed": True,
                        }
                        with open(path, "w", encoding="utf-8") as f:
                            json.dump(summary, f, ensure_ascii=False, indent=2)
                        compressed += 1
            except (ValueError, OSError):
                continue

        if compressed:
            logger.info(f"Compressed {compressed} old memory records")
        return compressed
