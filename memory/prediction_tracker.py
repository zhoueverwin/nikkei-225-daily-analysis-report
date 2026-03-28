"""Prediction accuracy tracker - verifies past predictions against actual results."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


DIRECTION_MAP = {
    "strong_up": 2,
    "slight_up": 1,
    "flat": 0,
    "slight_down": -1,
    "strong_down": -2,
}


class PredictionTracker:
    """Tracks and verifies prediction accuracy."""

    def verify_prediction(self, prediction: dict, actual_data: dict) -> dict[str, Any]:
        """Verify a single prediction against actual market data.

        Args:
            prediction: Previous day's prediction dict
            actual_data: Today's actual market data

        Returns:
            Verification result dict
        """
        target = prediction.get("target", "")
        predicted = prediction.get("prediction", "")
        confidence = prediction.get("confidence", "medium")

        actual_change_pct = 0.0

        if "nikkei" in target:
            nikkei = actual_data.get("market_data", {}).get("nikkei", {})
            actual_change_pct = nikkei.get("change_pct", 0)

        # Map actual change to category
        actual_direction = self._categorize_change(actual_change_pct)

        # Check if direction was correct
        pred_sign = DIRECTION_MAP.get(predicted, 0)
        actual_sign = DIRECTION_MAP.get(actual_direction, 0)

        if pred_sign == 0 and actual_sign == 0:
            result = "correct"
        elif pred_sign * actual_sign > 0:
            result = "correct"  # Same direction
        elif pred_sign == 0 or actual_sign == 0:
            result = "partial"  # One was flat
        else:
            result = "incorrect"  # Opposite direction

        return {
            "prediction_date": prediction.get("date", "unknown"),
            "target": target,
            "predicted": predicted,
            "actual": actual_direction,
            "actual_change_pct": round(actual_change_pct, 2),
            "result": result,
            "confidence_was": confidence,
        }

    def _categorize_change(self, change_pct: float) -> str:
        """Categorize a percentage change into direction labels."""
        if change_pct > 1.5:
            return "strong_up"
        elif change_pct > 0.3:
            return "slight_up"
        elif change_pct > -0.3:
            return "flat"
        elif change_pct > -1.5:
            return "slight_down"
        else:
            return "strong_down"

    def calculate_accuracy(self, verifications: list[dict]) -> dict[str, Any]:
        """Calculate accuracy statistics from verification results.

        Args:
            verifications: List of verification result dicts

        Returns:
            Accuracy statistics
        """
        if not verifications:
            return {
                "total": 0,
                "correct": 0,
                "partial": 0,
                "incorrect": 0,
                "accuracy_pct": None,
                "by_confidence": {},
            }

        total = len(verifications)
        correct = sum(1 for v in verifications if v["result"] == "correct")
        partial = sum(1 for v in verifications if v["result"] == "partial")
        incorrect = sum(1 for v in verifications if v["result"] == "incorrect")

        # Accuracy by confidence level
        by_confidence: dict[str, dict] = {}
        for conf in ("high", "medium", "low"):
            conf_items = [v for v in verifications if v.get("confidence_was") == conf]
            if conf_items:
                conf_correct = sum(1 for v in conf_items if v["result"] == "correct")
                by_confidence[conf] = {
                    "total": len(conf_items),
                    "correct": conf_correct,
                    "accuracy_pct": round(conf_correct / len(conf_items) * 100, 1),
                }

        return {
            "total": total,
            "correct": correct,
            "partial": partial,
            "incorrect": incorrect,
            "accuracy_pct": round(correct / total * 100, 1) if total > 0 else None,
            "by_confidence": by_confidence,
        }
