"""Historical report index generator."""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

INDEX_TEMPLATE = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>日経225分析レポート — アーカイブ</title>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d28; --surface2: #242836;
    --border: #2e3347; --accent: #6c8cff; --accent2: #ff6c8c;
    --accent3: #6cffa8; --text: #e0e4f0; --text2: #8b92a8;
    --red: #ff4d6a; --green: #4dff91;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: 'Segoe UI', -apple-system, 'Hiragino Sans', 'Noto Sans JP', sans-serif;
    background: var(--bg); color: var(--text); line-height: 1.7;
  }}
  .header {{
    background: linear-gradient(135deg, #1a1d28 0%, #0f1117 50%, #1a1028 100%);
    border-bottom: 1px solid var(--border); padding: 40px 30px; text-align: center;
  }}
  .header h1 {{
    font-size: 1.8em;
    background: linear-gradient(135deg, var(--accent), var(--accent2));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  }}
  .header .subtitle {{ color: var(--text2); margin-top: 8px; }}
  .container {{ max-width: 800px; margin: 0 auto; padding: 30px 20px; }}
  .latest-link {{
    display: block; text-align: center; padding: 16px;
    background: var(--surface); border: 1px solid var(--accent);
    border-radius: 10px; margin-bottom: 30px;
    color: var(--accent); text-decoration: none; font-weight: 600;
  }}
  .latest-link:hover {{ background: var(--surface2); }}
  .report-card {{
    display: flex; align-items: center; justify-content: space-between;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px 20px; margin-bottom: 8px;
    text-decoration: none; color: var(--text); transition: border-color 0.2s;
  }}
  .report-card:hover {{ border-color: var(--accent); }}
  .report-date {{ font-weight: 600; color: var(--accent); min-width: 110px; }}
  .report-day {{ color: var(--text2); font-size: 0.85em; min-width: 40px; }}
  .report-spacer {{ flex: 1; }}
  .report-link {{ color: var(--accent); font-size: 0.85em; }}
  .month-header {{
    color: var(--text2); font-size: 0.9em; font-weight: 600;
    margin: 20px 0 10px; padding-bottom: 6px; border-bottom: 1px solid var(--border);
  }}
  .footer {{
    text-align: center; padding: 30px; color: var(--text2);
    font-size: 0.8em; border-top: 1px solid var(--border); margin-top: 30px;
  }}
</style>
</head>
<body>
<div class="header">
  <h1>日経225 分析レポート アーカイブ</h1>
  <div class="subtitle">{report_count}件のレポート</div>
</div>
<div class="container">
  <a class="latest-link" href="index.html">最新のレポートを見る →</a>
  {report_list}
</div>
<div class="footer">日経225毎日分析レポート — AI自動生成</div>
</body>
</html>"""

DAY_NAMES_JA = ["月", "火", "水", "木", "金", "土", "日"]


class IndexGenerator:
    """Generates an archive index page listing all historical reports."""

    def __init__(self, docs_dir: str = "docs"):
        self.docs_dir = Path(docs_dir)

    def generate(self) -> Path:
        """Scan docs/ for reports and generate archive.html."""
        reports = self._scan_reports()

        if not reports:
            logger.warning("No reports found to index")
            return self.docs_dir / "archive.html"

        report_list_html = self._build_report_list(reports)
        html = INDEX_TEMPLATE.format(
            report_count=len(reports),
            report_list=report_list_html,
        )

        output_path = self.docs_dir / "archive.html"
        output_path.write_text(html, encoding="utf-8")
        logger.info(f"Archive index generated: {output_path} ({len(reports)} reports)")
        return output_path

    def _scan_reports(self) -> list[dict[str, str]]:
        """Find all report-YYYY-MM-DD.html files."""
        reports = []
        pattern = re.compile(r"report-(\d{4}-\d{2}-\d{2})\.html")

        for path in sorted(self.docs_dir.glob("report-*.html"), reverse=True):
            match = pattern.match(path.name)
            if match:
                date_str = match.group(1)
                try:
                    date = datetime.strptime(date_str, "%Y-%m-%d")
                    reports.append({
                        "date": date_str,
                        "filename": path.name,
                        "day_of_week": DAY_NAMES_JA[date.weekday()],
                        "month": date.strftime("%Y年%m月"),
                    })
                except ValueError:
                    continue

        return reports

    def _build_report_list(self, reports: list[dict]) -> str:
        """Build HTML for the report list, grouped by month."""
        html_parts = []
        current_month = ""

        for r in reports:
            if r["month"] != current_month:
                current_month = r["month"]
                html_parts.append(f'<div class="month-header">{current_month}</div>')

            html_parts.append(
                f'<a class="report-card" href="{r["filename"]}">'
                f'<span class="report-date">{r["date"]}</span>'
                f'<span class="report-day">({r["day_of_week"]})</span>'
                f'<span class="report-spacer"></span>'
                f'<span class="report-link">レポートを見る →</span>'
                f'</a>'
            )

        return "\n".join(html_parts)
