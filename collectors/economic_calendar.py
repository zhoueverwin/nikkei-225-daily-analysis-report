"""Economic event calendar — tracks upcoming market-moving events."""

import logging
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Major recurring economic events with their typical schedule
# Format: (name_ja, name_en, frequency, typical_day_description, importance)
RECURRING_EVENTS = [
    # US Events
    {
        "name_ja": "米国雇用統計（非農業部門）",
        "name_en": "US Non-Farm Payrolls",
        "frequency": "monthly",
        "typical_timing": "毎月第1金曜日",
        "importance": "high",
        "impact_ja": "米国の雇用状況はFRBの金融政策に直結。予想との乖離で為替・株が大きく動く",
        "category": "us_macro",
    },
    {
        "name_ja": "米国CPI（消費者物価指数）",
        "name_en": "US CPI",
        "frequency": "monthly",
        "typical_timing": "毎月10-14日頃",
        "importance": "high",
        "impact_ja": "インフレ動向の最重要指標。利上げ/利下げ判断に直結",
        "category": "us_macro",
    },
    {
        "name_ja": "FOMC金利決定・声明",
        "name_en": "FOMC Decision",
        "frequency": "6_weeks",
        "typical_timing": "年8回（約6週間ごと）",
        "importance": "critical",
        "impact_ja": "世界の金融市場の方向性を決定する最重要イベント",
        "category": "us_policy",
    },
    {
        "name_ja": "米国PCEデフレーター",
        "name_en": "US PCE Price Index",
        "frequency": "monthly",
        "typical_timing": "毎月末",
        "importance": "high",
        "impact_ja": "FRBが重視するインフレ指標。CPIと並ぶ重要度",
        "category": "us_macro",
    },
    {
        "name_ja": "米国GDP速報値",
        "name_en": "US GDP (Advance)",
        "frequency": "quarterly",
        "typical_timing": "四半期末の翌月末",
        "importance": "high",
        "impact_ja": "米国経済の全体像。景気後退懸念の判断材料",
        "category": "us_macro",
    },
    {
        "name_ja": "米国ISM製造業景気指数",
        "name_en": "US ISM Manufacturing PMI",
        "frequency": "monthly",
        "typical_timing": "毎月第1営業日",
        "importance": "medium",
        "impact_ja": "製造業の景況感。50を境に拡大/縮小を判断",
        "category": "us_macro",
    },
    # Japan Events
    {
        "name_ja": "日銀金融政策決定会合",
        "name_en": "BOJ Policy Decision",
        "frequency": "6_weeks",
        "typical_timing": "年8回",
        "importance": "critical",
        "impact_ja": "日本の金融政策の根幹。利上げ/YCC調整で為替・株に甚大な影響",
        "category": "jp_policy",
    },
    {
        "name_ja": "日本CPI（全国消費者物価指数）",
        "name_en": "Japan CPI",
        "frequency": "monthly",
        "typical_timing": "毎月第3金曜日頃",
        "importance": "high",
        "impact_ja": "日銀の政策判断材料。インフレ持続性の見極め",
        "category": "jp_macro",
    },
    {
        "name_ja": "日銀短観",
        "name_en": "BOJ Tankan Survey",
        "frequency": "quarterly",
        "typical_timing": "4月/7月/10月/12月初旬",
        "importance": "high",
        "impact_ja": "日本企業の景況感の最も信頼性の高い指標",
        "category": "jp_macro",
    },
    {
        "name_ja": "日本GDP速報値",
        "name_en": "Japan GDP (Preliminary)",
        "frequency": "quarterly",
        "typical_timing": "四半期末の翌々月中旬",
        "importance": "medium",
        "impact_ja": "日本経済の全体像。テクニカルリセッション判定にも使用",
        "category": "jp_macro",
    },
    {
        "name_ja": "日本貿易統計",
        "name_en": "Japan Trade Balance",
        "frequency": "monthly",
        "typical_timing": "毎月下旬",
        "importance": "medium",
        "impact_ja": "輸出入動向。円安の恩恵/原油高の影響が反映",
        "category": "jp_macro",
    },
    # Other Major Events
    {
        "name_ja": "ECB金利決定",
        "name_en": "ECB Rate Decision",
        "frequency": "6_weeks",
        "typical_timing": "年8回",
        "importance": "medium",
        "impact_ja": "欧州の金融政策。ユーロ円を通じて間接的に影響",
        "category": "global_policy",
    },
    {
        "name_ja": "中国PMI（製造業購買担当者指数）",
        "name_en": "China Manufacturing PMI",
        "frequency": "monthly",
        "typical_timing": "毎月末〜翌月1日",
        "importance": "medium",
        "impact_ja": "中国経済の先行指標。日本の輸出企業に影響",
        "category": "global_macro",
    },
    {
        "name_ja": "OPEC+会合",
        "name_en": "OPEC+ Meeting",
        "frequency": "irregular",
        "typical_timing": "不定期（概ね月1回）",
        "importance": "high",
        "impact_ja": "原油減産/増産の決定。エネルギー価格とインフレ見通しに直結",
        "category": "commodities",
    },
    # Seasonal Japan Events
    {
        "name_ja": "日本企業決算発表シーズン",
        "name_en": "Japan Earnings Season",
        "frequency": "quarterly",
        "typical_timing": "1月下旬/4月下旬/7月下旬/10月下旬〜",
        "importance": "high",
        "impact_ja": "個別銘柄・セクターの業績で板块ごとに大きく動く時期",
        "category": "jp_corporate",
    },
    {
        "name_ja": "メジャーSQ（特別清算指数算出日）",
        "name_en": "Major SQ (Special Quotation)",
        "frequency": "quarterly",
        "typical_timing": "3月/6月/9月/12月の第2金曜日",
        "importance": "medium",
        "impact_ja": "先物・オプションの清算で出来高急増、価格変動が大きくなりやすい",
        "category": "jp_market_structure",
    },
]

# Known specific dates for 2026 (manually maintained — update periodically)
KNOWN_EVENTS_2026: list[dict[str, str]] = [
    # Q1 2026 remaining
    {"date": "2026-03-28", "name_ja": "米国PCEデフレーター（2月分）", "importance": "high", "category": "us_macro"},
    {"date": "2026-03-31", "name_ja": "日銀短観（3月調査）", "importance": "high", "category": "jp_macro"},
    {"date": "2026-03-31", "name_ja": "中国PMI（3月）", "importance": "medium", "category": "global_macro"},
    # Q2 2026
    {"date": "2026-04-01", "name_ja": "米国ISM製造業（3月）", "importance": "medium", "category": "us_macro"},
    {"date": "2026-04-03", "name_ja": "米国雇用統計（3月）", "importance": "high", "category": "us_macro"},
    {"date": "2026-04-10", "name_ja": "米国CPI（3月）", "importance": "high", "category": "us_macro"},
    {"date": "2026-04-17", "name_ja": "ECB金利決定", "importance": "medium", "category": "global_policy"},
    {"date": "2026-04-24", "name_ja": "日本CPI（3月・全国）", "importance": "high", "category": "jp_macro"},
    {"date": "2026-04-28", "name_ja": "日銀金融政策決定会合", "importance": "critical", "category": "jp_policy"},
    {"date": "2026-04-30", "name_ja": "米国PCEデフレーター（3月）/ FOMC", "importance": "critical", "category": "us_policy"},
    {"date": "2026-05-01", "name_ja": "米国ISM製造業（4月）", "importance": "medium", "category": "us_macro"},
    {"date": "2026-05-08", "name_ja": "米国雇用統計（4月）", "importance": "high", "category": "us_macro"},
    {"date": "2026-05-13", "name_ja": "米国CPI（4月）", "importance": "high", "category": "us_macro"},
    {"date": "2026-06-10", "name_ja": "米国CPI（5月）", "importance": "high", "category": "us_macro"},
    {"date": "2026-06-12", "name_ja": "メジャーSQ", "importance": "medium", "category": "jp_market_structure"},
    {"date": "2026-06-17", "name_ja": "FOMC金利決定", "importance": "critical", "category": "us_policy"},
    {"date": "2026-06-18", "name_ja": "日銀金融政策決定会合", "importance": "critical", "category": "jp_policy"},
]


class EconomicCalendar:
    """Provides upcoming economic events relevant to Japanese markets."""

    def __init__(self):
        self.known_events = KNOWN_EVENTS_2026
        self.recurring_events = RECURRING_EVENTS

    def get_upcoming_events(self, days_ahead: int = 14) -> dict[str, Any]:
        """Get events happening in the next N days."""
        today = datetime.now().date()
        cutoff = today + timedelta(days=days_ahead)

        upcoming = []
        for event in self.known_events:
            try:
                event_date = datetime.strptime(event["date"], "%Y-%m-%d").date()
                if today <= event_date <= cutoff:
                    days_until = (event_date - today).days
                    upcoming.append({
                        **event,
                        "days_until": days_until,
                        "urgency": "imminent" if days_until <= 2 else ("soon" if days_until <= 7 else "upcoming"),
                    })
            except ValueError:
                continue

        # Sort by date
        upcoming.sort(key=lambda x: x["date"])

        # Separate by importance
        critical = [e for e in upcoming if e.get("importance") == "critical"]
        high = [e for e in upcoming if e.get("importance") == "high"]
        medium = [e for e in upcoming if e.get("importance") == "medium"]

        return {
            "period": f"{today} ~ {cutoff}",
            "total_events": len(upcoming),
            "events": upcoming,
            "critical_events": critical,
            "high_importance": high,
            "medium_importance": medium,
            "next_critical": critical[0] if critical else None,
        }

    def get_recurring_reference(self) -> list[dict[str, str]]:
        """Get the reference list of recurring events."""
        return [
            {
                "name_ja": e["name_ja"],
                "timing": e["typical_timing"],
                "importance": e["importance"],
                "impact_ja": e["impact_ja"],
            }
            for e in self.recurring_events
        ]

    def get_this_week_events(self) -> list[dict[str, Any]]:
        """Get events for the current week (Mon-Fri)."""
        today = datetime.now().date()
        # Find this Monday
        monday = today - timedelta(days=today.weekday())
        friday = monday + timedelta(days=4)

        week_events = []
        for event in self.known_events:
            try:
                event_date = datetime.strptime(event["date"], "%Y-%m-%d").date()
                if monday <= event_date <= friday:
                    week_events.append({
                        **event,
                        "day_of_week": ["月", "火", "水", "木", "金"][event_date.weekday()],
                        "is_today": event_date == today,
                    })
            except ValueError:
                continue

        return sorted(week_events, key=lambda x: x["date"])
