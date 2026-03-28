"""News sentiment analyzer — keyword-based scoring without LLM dependency."""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Sentiment keyword dictionaries (English + Japanese)
POSITIVE_KEYWORDS = {
    # English
    "surge": 2, "soar": 2, "rally": 2, "boom": 2, "breakthrough": 2,
    "gain": 1, "rise": 1, "up": 0.5, "growth": 1, "recover": 1.5,
    "strong": 1, "bullish": 1.5, "optimism": 1.5, "beat": 1, "exceed": 1,
    "record high": 2, "upgrade": 1.5, "stimulus": 1, "easing": 1,
    "deal": 1, "agreement": 1, "peace": 1.5, "ceasefire": 2,
    # Japanese
    "上昇": 1, "急騰": 2, "反発": 1.5, "回復": 1, "好調": 1.5,
    "最高値": 2, "強気": 1.5, "買い": 0.5, "成長": 1, "増益": 1.5,
    "緩和": 1, "合意": 1, "改善": 1, "堅調": 1,
}

NEGATIVE_KEYWORDS = {
    # English
    "crash": -3, "plunge": -2.5, "collapse": -2.5, "crisis": -2,
    "war": -2, "conflict": -2, "sanctions": -1.5, "tariff": -1.5,
    "fall": -1, "drop": -1, "decline": -1, "loss": -1, "down": -0.5,
    "fear": -1.5, "panic": -2, "bearish": -1.5, "recession": -2,
    "inflation": -1, "default": -2, "bankruptcy": -2, "layoff": -1.5,
    "attack": -2, "strike": -1.5, "escalation": -2, "tension": -1.5,
    "sell-off": -2, "selloff": -2, "slump": -1.5, "warning": -1,
    "downgrade": -1.5, "risk": -0.5, "uncertainty": -1,
    "iran": -1.5, "missile": -2, "nuclear": -1.5, "invasion": -2.5,
    # Japanese
    "下落": -1, "急落": -2, "暴落": -3, "戦争": -2, "紛争": -2,
    "制裁": -1.5, "関税": -1.5, "不安": -1.5, "恐怖": -2,
    "弱気": -1.5, "売り": -0.5, "後退": -1, "減益": -1.5,
    "危機": -2, "リスク": -0.5, "緊張": -1.5, "懸念": -1,
    "インフレ": -1, "破綻": -2, "地政学": -1.5,
}


class SentimentAnalyzer:
    """Analyzes news sentiment using keyword scoring."""

    def analyze_article(self, article: dict) -> dict[str, Any]:
        """Score a single article's sentiment."""
        text = f"{article.get('title', '')} {article.get('description', '')}".lower()

        pos_score = 0.0
        neg_score = 0.0
        pos_matches = []
        neg_matches = []

        for keyword, weight in POSITIVE_KEYWORDS.items():
            count = text.count(keyword.lower())
            if count > 0:
                pos_score += weight * count
                pos_matches.append(keyword)

        for keyword, weight in NEGATIVE_KEYWORDS.items():
            count = text.count(keyword.lower())
            if count > 0:
                neg_score += weight * count
                neg_matches.append(keyword)

        total = pos_score + neg_score  # neg_score is already negative
        # Normalize to -1 to +1 range
        max_possible = max(abs(pos_score), abs(neg_score), 1)
        normalized = total / (max_possible * 2)
        normalized = max(-1.0, min(1.0, normalized))

        return {
            "title": article.get("title", ""),
            "sentiment_score": round(normalized, 3),
            "positive_signals": pos_matches,
            "negative_signals": neg_matches,
            "raw_positive": round(pos_score, 2),
            "raw_negative": round(neg_score, 2),
        }

    def analyze_batch(self, articles: list[dict]) -> dict[str, Any]:
        """Analyze sentiment across multiple articles."""
        if not articles:
            return {
                "overall_score": 0.0,
                "overall_label": "neutral",
                "article_count": 0,
                "articles": [],
                "top_positive": [],
                "top_negative": [],
                "keyword_frequency": {},
            }

        scored = [self.analyze_article(a) for a in articles]

        # Overall sentiment (weighted average)
        scores = [s["sentiment_score"] for s in scored]
        overall = sum(scores) / len(scores) if scores else 0.0

        # Label
        if overall > 0.2:
            label = "positive"
        elif overall > 0.05:
            label = "slightly_positive"
        elif overall > -0.05:
            label = "neutral"
        elif overall > -0.2:
            label = "slightly_negative"
        else:
            label = "negative"

        # Top positive/negative articles
        sorted_by_score = sorted(scored, key=lambda x: x["sentiment_score"])
        top_negative = sorted_by_score[:3]
        top_positive = sorted_by_score[-3:][::-1]

        # Keyword frequency
        keyword_freq: dict[str, int] = {}
        for s in scored:
            for kw in s["positive_signals"] + s["negative_signals"]:
                keyword_freq[kw] = keyword_freq.get(kw, 0) + 1

        return {
            "overall_score": round(overall, 3),
            "overall_label": label,
            "article_count": len(scored),
            "articles": scored,
            "top_positive": top_positive,
            "top_negative": top_negative,
            "keyword_frequency": dict(sorted(keyword_freq.items(), key=lambda x: x[1], reverse=True)[:15]),
        }

    def generate_summary_ja(self, analysis: dict) -> str:
        """Generate a Japanese summary of the sentiment analysis."""
        score = analysis["overall_score"]
        count = analysis["article_count"]
        label = analysis["overall_label"]

        label_ja = {
            "positive": "ポジティブ",
            "slightly_positive": "やや ポジティブ",
            "neutral": "中立",
            "slightly_negative": "ややネガティブ",
            "negative": "ネガティブ",
        }.get(label, "中立")

        top_neg_titles = [a["title"][:60] for a in analysis.get("top_negative", []) if a["sentiment_score"] < -0.1]
        top_pos_titles = [a["title"][:60] for a in analysis.get("top_positive", []) if a["sentiment_score"] > 0.1]

        summary = f"ニュース感情分析: {label_ja} (スコア: {score:+.3f}, {count}記事分析)"

        if top_neg_titles:
            summary += f"\n主なネガティブ要因: {'; '.join(top_neg_titles[:2])}"
        if top_pos_titles:
            summary += f"\n主なポジティブ要因: {'; '.join(top_pos_titles[:2])}"

        return summary
