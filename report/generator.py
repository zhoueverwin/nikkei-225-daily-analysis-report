"""Report generator - assembles analysis data into HTML report."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger(__name__)


class ReportGenerator:
    """Generates HTML reports from analysis results."""

    def __init__(self, template_dir: str = "report/templates", output_dir: str = "docs"):
        self.template_dir = Path(template_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            autoescape=False,
        )

    def generate(
        self,
        date: str,
        market_data: dict[str, Any],
        macro_data: dict[str, Any],
        technical_analysis: dict[str, Any] | None,
        macro_linkage: dict[str, Any],
        llm_analysis: dict[str, Any],
        prediction_accuracy: dict[str, Any] | None,
        prediction_verification: dict[str, Any] | None,
        data_quality: dict[str, Any],
        contradiction_flags: list[dict],
        # Phase 2+3 additions
        news_data: dict[str, Any] | None = None,
        sentiment_data: dict[str, Any] | None = None,
        sector_data: dict[str, Any] | None = None,
        calendar_data: dict[str, Any] | None = None,
        candlestick_data: list[dict] | None = None,
        volume_data: list[dict] | None = None,
    ) -> Path:
        """Generate the HTML report and return the output path."""

        nikkei = market_data.get("market_data", {}).get("nikkei", {})

        context = {
            "date": date,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M JST"),
            "nikkei": nikkei,
            "market_data": market_data.get("market_data", {}),
            "fx": market_data.get("fx", {}),
            "commodities": market_data.get("commodities", {}),
            "bonds": {**market_data.get("bonds", {}), **macro_data.get("bonds", {})},
            "technical": technical_analysis,
            "macro_linkage": macro_linkage,
            "analysis": llm_analysis,
            "prediction_accuracy": prediction_accuracy,
            "prediction_verification": prediction_verification,
            "data_quality": data_quality,
            "contradiction_flags": contradiction_flags,
            # Phase 2+3
            "news_data": news_data,
            "sentiment_data": sentiment_data,
            "sector_data": sector_data,
            "calendar_data": calendar_data,
            "candlestick_data": candlestick_data,
            "volume_data": volume_data,
        }

        template = self.env.get_template("report.html.j2")
        html = template.render(**context)

        # Save as dated file and as index.html for GitHub Pages
        dated_path = self.output_dir / f"report-{date}.html"
        index_path = self.output_dir / "index.html"

        dated_path.write_text(html, encoding="utf-8")
        index_path.write_text(html, encoding="utf-8")

        logger.info(f"Report generated: {dated_path}")
        return dated_path
