"""Sector rotation analyzer for Japanese market sectors."""

import logging
from typing import Any

import yfinance as yf

logger = logging.getLogger(__name__)

# Japanese sector ETFs (traded on TSE) as proxies for sector performance
# Using NEXT FUNDS sector ETFs and major sector representatives
SECTOR_ETFS = {
    "半導体": {
        "symbol": "8035.T",  # Tokyo Electron (semiconductor bellwether)
        "name_ja": "半導体",
        "name_en": "Semiconductors",
    },
    "自動車": {
        "symbol": "7203.T",  # Toyota
        "name_ja": "自動車",
        "name_en": "Automobiles",
    },
    "銀行": {
        "symbol": "8306.T",  # MUFG
        "name_ja": "銀行",
        "name_en": "Banks",
    },
    "不動産": {
        "symbol": "8801.T",  # Mitsui Fudosan
        "name_ja": "不動産",
        "name_en": "Real Estate",
    },
    "電機": {
        "symbol": "6758.T",  # Sony
        "name_ja": "電機・エレクトロニクス",
        "name_en": "Electronics",
    },
    "商社": {
        "symbol": "8058.T",  # Mitsubishi Corp
        "name_ja": "総合商社",
        "name_en": "Trading Companies",
    },
    "医薬品": {
        "symbol": "4502.T",  # Takeda
        "name_ja": "医薬品",
        "name_en": "Pharmaceuticals",
    },
    "通信": {
        "symbol": "9432.T",  # NTT
        "name_ja": "通信",
        "name_en": "Telecom",
    },
    "鉄鋼": {
        "symbol": "5401.T",  # Nippon Steel
        "name_ja": "鉄鋼",
        "name_en": "Steel",
    },
    "電力": {
        "symbol": "9501.T",  # Tokyo Electric Power
        "name_ja": "電力・ガス",
        "name_en": "Utilities",
    },
}


class SectorRotationAnalyzer:
    """Analyzes sector rotation patterns in the Japanese market."""

    def __init__(self):
        self.sectors = SECTOR_ETFS

    def fetch_sector_data(self, symbol: str) -> dict[str, Any] | None:
        """Fetch latest price data for a sector proxy."""
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            if hist.empty or len(hist) < 2:
                return None

            latest = hist.iloc[-1]
            prev = hist.iloc[-2]
            close = float(latest["Close"])
            prev_close = float(prev["Close"])
            change_pct = (close - prev_close) / prev_close * 100

            # 5-day performance
            first = hist.iloc[0]
            five_day_change = (close - float(first["Close"])) / float(first["Close"]) * 100

            return {
                "close": round(close, 1),
                "change_pct": round(change_pct, 2),
                "five_day_change_pct": round(five_day_change, 2),
                "volume": int(latest["Volume"]),
            }
        except Exception as e:
            logger.warning(f"Failed to fetch sector data for {symbol}: {e}")
            return None

    def analyze(self) -> dict[str, Any]:
        """Run full sector rotation analysis."""
        sector_data: dict[str, Any] = {}
        errors = []

        for sector_key, config in self.sectors.items():
            data = self.fetch_sector_data(config["symbol"])
            if data:
                sector_data[sector_key] = {
                    **data,
                    "symbol": config["symbol"],
                    "name_ja": config["name_ja"],
                    "name_en": config["name_en"],
                }
            else:
                errors.append(f"sector.{sector_key}: failed")

        if not sector_data:
            return {
                "available": False,
                "sectors": {},
                "leaders": [],
                "laggards": [],
                "rotation_signal_ja": "セクターデータ取得不可",
                "errors": errors,
            }

        # Sort by daily change
        sorted_sectors = sorted(
            sector_data.items(),
            key=lambda x: x[1]["change_pct"],
            reverse=True,
        )

        leaders = [
            {"sector_ja": s[1]["name_ja"], "change_pct": s[1]["change_pct"], "symbol": s[1]["symbol"]}
            for s in sorted_sectors[:3]
        ]
        laggards = [
            {"sector_ja": s[1]["name_ja"], "change_pct": s[1]["change_pct"], "symbol": s[1]["symbol"]}
            for s in sorted_sectors[-3:]
        ]

        # 5-day momentum leaders
        sorted_5d = sorted(
            sector_data.items(),
            key=lambda x: x[1]["five_day_change_pct"],
            reverse=True,
        )
        momentum_leaders = [
            {"sector_ja": s[1]["name_ja"], "five_day_change_pct": s[1]["five_day_change_pct"]}
            for s in sorted_5d[:3]
        ]

        # Rotation signal analysis
        rotation_signal = self._detect_rotation(sorted_sectors, sorted_5d)

        # Spread analysis (dispersion between best and worst)
        best_change = sorted_sectors[0][1]["change_pct"]
        worst_change = sorted_sectors[-1][1]["change_pct"]
        spread = best_change - worst_change

        return {
            "available": True,
            "sectors": sector_data,
            "leaders": leaders,
            "laggards": laggards,
            "momentum_leaders_5d": momentum_leaders,
            "spread": round(spread, 2),
            "rotation_signal_ja": rotation_signal,
            "errors": errors,
        }

    def _detect_rotation(self, daily_sorted: list, weekly_sorted: list) -> str:
        """Detect sector rotation patterns and generate Japanese description."""
        if not daily_sorted:
            return "データ不足"

        top_daily = daily_sorted[0][1]["name_ja"]
        bottom_daily = daily_sorted[-1][1]["name_ja"]
        top_weekly = weekly_sorted[0][1]["name_ja"] if weekly_sorted else ""

        spread = daily_sorted[0][1]["change_pct"] - daily_sorted[-1][1]["change_pct"]

        signals = []

        # Defensive vs cyclical rotation
        defensive = {"医薬品", "通信", "電力・ガス"}
        cyclical = {"半導体", "自動車", "鉄鋼", "総合商社"}

        top3_names = {s[1]["name_ja"] for s in daily_sorted[:3]}
        bottom3_names = {s[1]["name_ja"] for s in daily_sorted[-3:]}

        if top3_names & defensive and bottom3_names & cyclical:
            signals.append("ディフェンシブセクターが優位 — リスク回避の資金シフト")
        elif top3_names & cyclical and bottom3_names & defensive:
            signals.append("シクリカルセクターが優位 — リスク選好の資金フロー")

        # High dispersion
        if spread > 3.0:
            signals.append(f"セクター間の格差が大きい（{spread:.1f}%幅）— 選別物色の傾向")
        elif spread < 1.0:
            signals.append(f"セクター間の格差が小さい（{spread:.1f}%幅）— 全面的な動き")

        # Consistency check (daily leader = weekly leader?)
        if top_daily == top_weekly:
            signals.append(f"{top_daily}が日次・週次ともに首位 — 持続的なトレンド")

        if not signals:
            signals.append(f"本日の主役は{top_daily}（上位）と{bottom_daily}（下位）")

        return "。".join(signals)
