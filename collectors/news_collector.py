"""News collector using RSS feeds — no API key required."""

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Free RSS feeds for financial news
RSS_FEEDS = {
    "reuters_markets": "https://www.rssboard.org/rss-news?feed=reuters&category=markets",
    "yahoo_finance_jp": "https://news.yahoo.co.jp/rss/topics/business.xml",
    "nhk_business": "https://www.nhk.or.jp/rss/news/cat5.xml",
    "nikkei_rss": "https://assets.wor.jp/rss/rdf/nikkei/news.rdf",
    "cnbc_world": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
    "reuters_jp": "https://assets.wor.jp/rss/rdf/reuters/top.rdf",
    "bloomberg_markets": "https://feeds.bloomberg.com/markets/news.rss",
}

# Keywords to filter relevant news
JAPAN_KEYWORDS = [
    "japan", "nikkei", "tokyo", "yen", "jpy", "boj", "bank of japan",
    "日経", "東京", "円", "日銀", "株", "市場", "為替",
    "topix", "sony", "toyota", "softbank", "semiconductor",
]

MARKET_KEYWORDS = [
    "oil", "crude", "opec", "iran", "war", "conflict", "sanctions",
    "fed", "inflation", "cpi", "rate", "tariff", "trade",
    "china", "recession", "gdp", "employment", "gold",
    "原油", "戦争", "制裁", "関税", "インフレ", "利上げ", "利下げ",
    "地政学", "紛争", "中東",
]


class NewsCollector:
    """Collects financial news from RSS feeds."""

    def __init__(self, timeout: int = 15):
        self.timeout = timeout

    def fetch_feed(self, url: str) -> list[dict[str, str]]:
        """Fetch and parse a single RSS feed."""
        articles = []
        try:
            resp = requests.get(url, timeout=self.timeout, headers={
                "User-Agent": "Nikkei225-Agent/1.0 (RSS Reader)"
            })
            resp.raise_for_status()

            root = ET.fromstring(resp.content)

            # Handle both RSS 2.0 and RDF/RSS 1.0
            items = root.findall(".//item")
            if not items:
                # Try with namespace for RDF
                ns = {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                      "rss": "http://purl.org/rss/1.0/",
                      "dc": "http://purl.org/dc/elements/1.1/"}
                items = root.findall(".//rss:item", ns)

            for item in items[:20]:  # Limit per feed
                title = self._get_text(item, "title")
                description = self._get_text(item, "description")
                link = self._get_text(item, "link")
                pub_date = self._get_text(item, "pubDate") or self._get_text(item, "{http://purl.org/dc/elements/1.1/}date")

                if title:
                    articles.append({
                        "title": title.strip(),
                        "description": (description or "").strip()[:300],
                        "link": (link or "").strip(),
                        "pub_date": (pub_date or "").strip(),
                        "source": url,
                    })

        except Exception as e:
            logger.warning(f"Failed to fetch RSS feed {url}: {e}")

        return articles

    def _get_text(self, element: ET.Element, tag: str) -> str | None:
        """Safely get text from an XML element."""
        el = element.find(tag)
        if el is not None and el.text:
            return el.text
        # Try with common namespaces
        for ns_prefix in ["", "{http://purl.org/rss/1.0/}", "{http://purl.org/dc/elements/1.1/}"]:
            el = element.find(f"{ns_prefix}{tag}")
            if el is not None and el.text:
                return el.text
        return None

    def collect_all(self) -> dict[str, Any]:
        """Collect news from all configured RSS feeds."""
        all_articles: list[dict[str, str]] = []

        for name, url in RSS_FEEDS.items():
            articles = self.fetch_feed(url)
            for a in articles:
                a["feed_name"] = name
            all_articles.extend(articles)
            if articles:
                logger.info(f"  {name}: {len(articles)} articles")
            else:
                logger.warning(f"  {name}: no articles (feed may be down)")

        # Filter for relevant articles
        japan_related = self._filter_articles(all_articles, JAPAN_KEYWORDS)
        market_related = self._filter_articles(all_articles, MARKET_KEYWORDS)

        # Deduplicate by title similarity
        japan_related = self._deduplicate(japan_related)
        market_related = self._deduplicate(market_related)

        # Combine and rank
        all_relevant = self._merge_and_rank(japan_related, market_related)

        return {
            "timestamp": datetime.now().isoformat(),
            "total_fetched": len(all_articles),
            "japan_related": japan_related[:15],
            "market_related": market_related[:15],
            "top_stories": all_relevant[:10],
            "feeds_status": {
                name: len(self.fetch_feed(url)) > 0 if False else "checked"
                for name, url in RSS_FEEDS.items()
            },
        }

    def _filter_articles(self, articles: list[dict], keywords: list[str]) -> list[dict]:
        """Filter articles matching any keyword."""
        result = []
        for article in articles:
            text = f"{article['title']} {article['description']}".lower()
            matched = [kw for kw in keywords if kw.lower() in text]
            if matched:
                article_copy = article.copy()
                article_copy["matched_keywords"] = matched
                result.append(article_copy)
        return result

    def _deduplicate(self, articles: list[dict]) -> list[dict]:
        """Remove near-duplicate articles by title similarity."""
        seen_titles: list[str] = []
        unique = []
        for article in articles:
            title = article["title"].lower()
            # Simple dedup: skip if >60% overlap with any seen title
            is_dup = False
            for seen in seen_titles:
                words_a = set(title.split())
                words_b = set(seen.split())
                if words_a and words_b:
                    overlap = len(words_a & words_b) / max(len(words_a), len(words_b))
                    if overlap > 0.6:
                        is_dup = True
                        break
            if not is_dup:
                unique.append(article)
                seen_titles.append(title)
        return unique

    def _merge_and_rank(self, japan: list[dict], market: list[dict]) -> list[dict]:
        """Merge and rank articles by relevance."""
        seen = set()
        merged = []
        # Japan-related first
        for a in japan:
            key = a["title"][:50]
            if key not in seen:
                a["relevance"] = "japan"
                merged.append(a)
                seen.add(key)
        # Then market-related
        for a in market:
            key = a["title"][:50]
            if key not in seen:
                a["relevance"] = "global_market"
                merged.append(a)
                seen.add(key)
        return merged
