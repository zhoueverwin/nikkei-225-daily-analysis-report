"""Contradiction detection engine - identifies conflicts between current data and historical stance."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ContradictionDetector:
    """Detects contradictions between current market data and historical analysis."""

    def detect(
        self,
        current_data: dict[str, Any],
        stance_history: list[dict[str, Any]],
        open_observations: list[str],
    ) -> list[dict[str, str]]:
        """Detect contradictions and return a list of flags.

        Args:
            current_data: Today's market data
            stance_history: Recent stance records from memory
            open_observations: Unresolved observations from previous day

        Returns:
            List of contradiction flag dicts with type, detail, severity
        """
        flags: list[dict[str, str]] = []

        if not stance_history:
            return flags

        latest_stance = stance_history[-1] if stance_history else None
        if not latest_stance:
            return flags

        # 1. Stance vs price movement contradiction
        stance_flag = self._check_stance_vs_price(current_data, latest_stance)
        if stance_flag:
            flags.append(stance_flag)

        # 2. Assumption invalidation
        assumption_flags = self._check_assumptions(current_data, latest_stance)
        flags.extend(assumption_flags)

        # 3. Consecutive direction mismatch
        streak_flag = self._check_direction_streak(current_data, stance_history)
        if streak_flag:
            flags.append(streak_flag)

        return flags

    def _check_stance_vs_price(self, data: dict, latest_stance: dict) -> dict[str, str] | None:
        """Check if market moved opposite to the stance."""
        stance = latest_stance.get("stance", {})
        direction = stance.get("direction", "")

        nikkei = data.get("market_data", {}).get("nikkei", {})
        change_pct = nikkei.get("change_pct", 0)

        # Bullish stance but significant drop
        if "bullish" in direction and change_pct < -2.0:
            return {
                "type": "stance_reversal",
                "type_ja": "スタンス逆転",
                "severity": "high",
                "detail_ja": f"前日の強気スタンスに反して日経は{change_pct:.2f}%下落。スタンスの見直しが必要",
            }
        # Bearish stance but significant rise
        elif "bearish" in direction and change_pct > 2.0:
            return {
                "type": "stance_reversal",
                "type_ja": "スタンス逆転",
                "severity": "high",
                "detail_ja": f"前日の弱気スタンスに反して日経は+{change_pct:.2f}%上昇。スタンスの見直しが必要",
            }
        return None

    def _check_assumptions(self, data: dict, latest_stance: dict) -> list[dict[str, str]]:
        """Check if key assumptions from previous analysis are still valid."""
        flags = []
        assumptions = latest_stance.get("stance", {}).get("key_assumptions", [])

        fx = data.get("fx", {}).get("usdjpy", {})
        fx_change = abs(fx.get("change_pct", 0))

        for assumption in assumptions:
            # Check for BOJ policy assumption vs large JPY moves
            if ("日銀" in assumption or "BOJ" in assumption.upper() or "宽松" in assumption or "緩和" in assumption):
                if fx_change > 1.5:
                    flags.append({
                        "type": "assumption_challenged",
                        "type_ja": "前提条件の変化",
                        "severity": "medium",
                        "detail_ja": f"前提条件「{assumption}」に関連: ドル円が{fx_change:.2f}%の大幅変動。政策前提の確認が必要",
                    })

        return flags

    def _check_direction_streak(self, data: dict, history: list[dict]) -> dict[str, str] | None:
        """Check if market has moved against stance for 3+ consecutive days."""
        if len(history) < 3:
            return None

        latest_stance = history[-1].get("stance", {}).get("direction", "")
        if "bullish" not in latest_stance and "bearish" not in latest_stance:
            return None

        # Count consecutive days against stance
        nikkei = data.get("market_data", {}).get("nikkei", {})
        today_down = nikkei.get("change_pct", 0) < 0

        if "bullish" in latest_stance and today_down:
            # Check if previous days also went down
            consecutive_misses = 0
            for rec in reversed(history[-3:]):
                conclusion = rec.get("core_conclusion", "")
                if "下落" in conclusion or "下げ" in conclusion:
                    consecutive_misses += 1
            if consecutive_misses >= 2:
                return {
                    "type": "consecutive_miss",
                    "type_ja": "連続予測ミス",
                    "severity": "high",
                    "detail_ja": f"強気スタンスにもかかわらず{consecutive_misses + 1}日連続下落。トレンド転換の可能性を検討すべき",
                }

        return None
