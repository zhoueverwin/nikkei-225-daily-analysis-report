"""Data collection fallback and degradation manager."""

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class DataQualityReport:
    """Tracks data collection completeness and quality."""

    def __init__(self):
        self.total_sources = 0
        self.successful = 0
        self.failed = 0
        self.degraded = 0  # using fallback
        self.errors: list[str] = []

    @property
    def completeness(self) -> float:
        if self.total_sources == 0:
            return 0.0
        return (self.successful + self.degraded) / self.total_sources * 100

    @property
    def is_sufficient(self) -> bool:
        """At least 80% data available to generate report."""
        return self.completeness >= 80.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_sources": self.total_sources,
            "successful": self.successful,
            "failed": self.failed,
            "degraded": self.degraded,
            "completeness_pct": round(self.completeness, 1),
            "is_sufficient": self.is_sufficient,
            "errors": self.errors,
        }


def assess_data_quality(market_data: dict, macro_data: dict) -> DataQualityReport:
    """Assess overall data collection quality."""
    report = DataQualityReport()

    # Count market data sources
    for category in ["market_data", "fx", "commodities", "bonds"]:
        items = market_data.get(category, {})
        for name, data in items.items():
            report.total_sources += 1
            if data:
                if isinstance(data, dict) and data.get("source") == "fallback":
                    report.degraded += 1
                else:
                    report.successful += 1
            else:
                report.failed += 1

    # Count macro data
    for category in ["macro", "bonds"]:
        items = macro_data.get(category, {})
        for name, data in items.items():
            report.total_sources += 1
            if data:
                report.successful += 1
            else:
                report.failed += 1

    # Collect all errors
    report.errors.extend(market_data.get("errors", []))
    report.errors.extend(macro_data.get("errors", []))

    return report
