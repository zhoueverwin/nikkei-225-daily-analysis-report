"""Macro economic data collector using FRED API."""

import logging
import os
from datetime import datetime, timedelta
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class MacroDataCollector:
    """Collects macro economic data from FRED."""

    def __init__(self, config_path: str = "config/data_sources.yaml"):
        self.api_key = os.environ.get("FRED_API_KEY")
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

    def _get_fred(self):
        """Lazy import and init FRED client."""
        if not self.api_key:
            logger.warning("FRED_API_KEY not set, macro data will be unavailable")
            return None
        from fredapi import Fred
        return Fred(api_key=self.api_key)

    def fetch_series(self, series_id: str, periods: int = 12) -> dict[str, Any] | None:
        """Fetch a FRED data series."""
        fred = self._get_fred()
        if not fred:
            return None
        try:
            data = fred.get_series(series_id)
            if data is None or data.empty:
                return None
            recent = data.dropna().tail(periods)
            latest = recent.iloc[-1]
            prev = recent.iloc[-2] if len(recent) > 1 else latest
            return {
                "series_id": series_id,
                "latest_value": float(latest),
                "latest_date": str(recent.index[-1].date()),
                "prev_value": float(prev),
                "change": float(latest - prev),
                "history": [
                    {"date": str(d.date()), "value": float(v)}
                    for d, v in recent.items()
                ],
            }
        except Exception as e:
            logger.error(f"Failed to fetch FRED series {series_id}: {e}")
            return None

    def collect_all(self) -> dict[str, Any]:
        """Collect all macro data series from config."""
        result: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "macro": {},
            "bonds": {},
            "errors": [],
        }

        # FRED macro series
        for series in self.config.get("macro", {}).get("fred_series", []):
            data = self.fetch_series(series["id"])
            if data:
                data["name"] = series["name"]
                result["macro"][series["id"]] = data
            else:
                result["errors"].append(f"macro.{series['id']}: failed")

        # JP 10Y bond from FRED
        bonds_cfg = self.config.get("bonds", {})
        jp_10y = bonds_cfg.get("jp_10y", {})
        if jp_10y.get("source") == "fred" and jp_10y.get("fred_series"):
            data = self.fetch_series(jp_10y["fred_series"])
            if data:
                data["name"] = "Japan 10Y Bond Yield"
                result["bonds"]["jp_10y"] = data

        return result
