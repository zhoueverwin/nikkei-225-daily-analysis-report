"""Telegram notification sender for report summaries."""

import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Sends report summaries to Telegram via Bot API."""

    def __init__(self):
        self.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.api_base = "https://api.telegram.org"

    @property
    def is_configured(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Send a text message to the configured chat."""
        if not self.is_configured:
            logger.warning("Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing)")
            return False

        url = f"{self.api_base}/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }

        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            if result.get("ok"):
                logger.info("Telegram notification sent successfully")
                return True
            else:
                logger.error(f"Telegram API error: {result}")
                return False
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}")
            return False

    def send_report_summary(
        self,
        date: str,
        nikkei_data: dict[str, Any],
        analysis: dict[str, Any],
        report_url: str | None = None,
    ) -> bool:
        """Send a formatted report summary to Telegram."""
        nikkei_close = nikkei_data.get("close", "N/A")
        nikkei_change = nikkei_data.get("change_pct", 0)
        change_emoji = "📈" if nikkei_change >= 0 else "📉"

        headline = analysis.get("headline_ja", "")
        prediction = analysis.get("prediction", {})
        pred_dir = prediction.get("direction", "")
        pred_conf = prediction.get("confidence", "")

        dir_map = {
            "strong_up": "強い上昇",
            "slight_up": "小幅上昇",
            "flat": "横ばい",
            "slight_down": "小幅下落",
            "strong_down": "強い下落",
        }

        message_lines = [
            f"<b>📊 日経225 日次レポート — {date}</b>",
            "",
            f"{change_emoji} <b>日経225: {nikkei_close:,.0f} ({nikkei_change:+.2f}%)</b>" if isinstance(nikkei_close, (int, float)) else f"{change_emoji} <b>日経225: {nikkei_close}</b>",
            "",
            f"📝 {headline}" if headline else "",
            "",
        ]

        # Key drivers
        drivers = analysis.get("drivers", [])
        if drivers:
            message_lines.append("<b>🔍 主要因:</b>")
            for d in drivers[:3]:
                impact_emoji = {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}.get(d.get("impact"), "⚪")
                message_lines.append(f"  {impact_emoji} {d.get('factor_ja', '')}")
            message_lines.append("")

        # Prediction
        if pred_dir:
            pred_text = dir_map.get(pred_dir, pred_dir)
            message_lines.append(f"<b>🔮 明日の予測:</b> {pred_text} (置信度: {pred_conf})")
            message_lines.append("")

        # Report link
        if report_url:
            message_lines.append(f"📄 <a href=\"{report_url}\">詳細レポートを見る</a>")

        message_lines.append("")
        message_lines.append("<i>※ 投資助言ではありません</i>")

        message = "\n".join(line for line in message_lines if line is not None)
        return self.send_message(message)


class LineNotifier:
    """Sends notifications via LINE Notify API."""

    def __init__(self):
        self.token = os.environ.get("LINE_NOTIFY_TOKEN")
        self.api_url = "https://notify-api.line.me/api/notify"

    @property
    def is_configured(self) -> bool:
        return bool(self.token)

    def send_message(self, message: str) -> bool:
        """Send a message via LINE Notify."""
        if not self.is_configured:
            logger.warning("LINE Notify not configured (LINE_NOTIFY_TOKEN missing)")
            return False

        try:
            resp = requests.post(
                self.api_url,
                headers={"Authorization": f"Bearer {self.token}"},
                data={"message": message},
                timeout=10,
            )
            resp.raise_for_status()
            logger.info("LINE notification sent successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to send LINE notification: {e}")
            return False

    def send_report_summary(
        self,
        date: str,
        nikkei_data: dict[str, Any],
        analysis: dict[str, Any],
        report_url: str | None = None,
    ) -> bool:
        """Send formatted summary via LINE."""
        nikkei_close = nikkei_data.get("close", "N/A")
        nikkei_change = nikkei_data.get("change_pct", 0)

        headline = analysis.get("headline_ja", "")
        prediction = analysis.get("prediction", {})

        dir_map = {
            "strong_up": "強い上昇", "slight_up": "小幅上昇", "flat": "横ばい",
            "slight_down": "小幅下落", "strong_down": "強い下落",
        }

        lines = [
            f"\n📊 日経225 日次レポート — {date}",
            f"日経225: {nikkei_close:,.0f} ({nikkei_change:+.2f}%)" if isinstance(nikkei_close, (int, float)) else f"日経225: {nikkei_close}",
            "",
            headline,
            "",
            f"明日の予測: {dir_map.get(prediction.get('direction', ''), '不明')} (置信度: {prediction.get('confidence', 'N/A')})",
        ]

        if report_url:
            lines.append(f"\n詳細: {report_url}")

        return self.send_message("\n".join(lines))
