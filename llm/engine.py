"""LLM engine - wraps Claude API for analysis generation."""

import json
import logging
import os
from pathlib import Path
from typing import Any

import anthropic

logger = logging.getLogger(__name__)


class LLMEngine:
    """Handles LLM-based analysis using Claude API."""

    def __init__(self, model: str = "claude-sonnet-4-6", max_tokens: int = 8192, temperature: float = 0.3):
        self.client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.prompts_dir = Path(__file__).parent / "prompts"

    def _load_prompt(self, name: str) -> str:
        """Load a prompt template from the prompts directory."""
        path = self.prompts_dir / f"{name}.txt"
        if not path.exists():
            raise FileNotFoundError(f"Prompt template not found: {path}")
        return path.read_text(encoding="utf-8")

    def generate_analysis(
        self,
        market_data: dict[str, Any],
        macro_data: dict[str, Any],
        technical_analysis: dict[str, Any] | None,
        macro_linkage: dict[str, Any],
        memory_context: dict[str, Any],
        contradiction_flags: list[dict],
        prediction_verification: dict[str, Any] | None,
        data_quality: dict[str, Any],
    ) -> dict[str, Any]:
        """Generate the main analysis using Claude.

        Returns a structured JSON with analysis sections in Japanese.
        """
        system_prompt = self._load_prompt("analysis")

        # Build user message with all data
        user_data = {
            "date": market_data.get("timestamp", ""),
            "market_data": {
                k: v for k, v in market_data.items()
                if k not in ("timestamp", "errors")
            },
            "macro_data": {
                k: v for k, v in macro_data.items()
                if k not in ("timestamp", "errors")
            },
            "technical_analysis": technical_analysis,
            "macro_linkage": macro_linkage,
            "memory": memory_context,
            "contradiction_flags": contradiction_flags,
            "prediction_verification": prediction_verification,
            "data_quality": data_quality,
        }

        user_message = f"""以下のデータに基づいて、本日の日経225分析レポートを生成してください。

## 入力データ
```json
{json.dumps(user_data, ensure_ascii=False, indent=2)}
```

上記のデータを分析し、指定されたJSON形式で出力してください。"""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                system=system_prompt,
                messages=[{"role": "user", "content": user_message}],
            )

            # Extract JSON from response
            text = response.content[0].text
            # Try to parse as JSON directly, or extract from code block
            result = self._parse_json_response(text)

            # Promote beginner_explanation_ja → beginner_lesson for downstream compatibility
            if "beginner_explanation_ja" in result and not result.get("beginner_lesson"):
                result["beginner_lesson"] = result.pop("beginner_explanation_ja")

            return result

        except anthropic.APIError as e:
            logger.error(f"Claude API error: {e}")
            return self._fallback_analysis(market_data)
        except Exception as e:
            logger.error(f"Analysis generation failed: {e}")
            return self._fallback_analysis(market_data)

    def generate_beginner_lesson(self, analysis_context: str) -> dict[str, str]:
        """Generate a beginner-friendly lesson based on today's key topic."""
        system_prompt = self._load_prompt("beginner")

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2048,
                temperature=0.5,
                system=system_prompt,
                messages=[{"role": "user", "content": f"今日の分析コンテキスト:\n{analysis_context}"}],
            )
            text = response.content[0].text
            return self._parse_json_response(text)
        except Exception as e:
            logger.error(f"Beginner lesson generation failed: {e}")
            return {
                "topic_ja": "本日のトピック",
                "explanation_ja": "データ取得エラーのため、本日のレッスンは省略します。",
            }

    def _parse_json_response(self, text: str) -> dict[str, Any]:
        """Parse JSON from LLM response, handling code blocks."""
        # Try direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try extracting from ```json ... ``` block
        if "```json" in text:
            start = text.index("```json") + 7
            end = text.index("```", start)
            return json.loads(text[start:end].strip())

        # Try extracting from ``` ... ```
        if "```" in text:
            start = text.index("```") + 3
            end = text.index("```", start)
            return json.loads(text[start:end].strip())

        raise ValueError(f"Could not parse JSON from LLM response: {text[:200]}...")

    def _fallback_analysis(self, market_data: dict) -> dict[str, Any]:
        """Generate a data-only fallback when LLM is unavailable."""
        nikkei = market_data.get("market_data", {}).get("nikkei", {})
        return {
            "headline_ja": f"日経225: {nikkei.get('close', 'N/A')} ({nikkei.get('change_pct', 0):+.2f}%)",
            "summary_ja": "LLM分析エンジンが利用できないため、データのみの表示となります。",
            "drivers": [],
            "prediction": {
                "direction": "unknown",
                "confidence": "low",
                "reasoning_ja": "分析エンジン停止中",
            },
            "stance": {
                "direction": "neutral",
                "confidence": "low",
                "key_assumptions": [],
            },
            "open_observations": [],
            "risk_events": [],
            "beginner_lesson": None,
            "_fallback": True,
        }
