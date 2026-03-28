"""News collector — hybrid Scrapling + RSS for maximum coverage."""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

# --- Scrapling sources (static HTML sites that don't need JS rendering) ---
SCRAPE_SOURCES = {
    "cnbc_world": {
        "url": "https://www.cnbc.com/world/?region=world",
        "selectors": ["h2 a", "h3 a"],
        "name_ja": "CNBC",
        "lang": "en",
    },
    "nikkei_economy": {
        "url": "https://www.nikkei.com/economy/",
        "selectors": ["a[href*='/article/']"],
        "name_ja": "日経新聞",
        "lang": "ja",
    },
    "investing_com": {
        "url": "https://www.investing.com/news/stock-market-news",
        "selectors": ["a.title", "a[data-test='article-title-link']", "article a"],
        "name_ja": "Investing.com",
        "lang": "en",
    },
}

# --- RSS sources (reliable, always structured) ---
RSS_SOURCES = {
    "nhk_business": {
        "url": "https://www.nhk.or.jp/rss/news/cat5.xml",
        "name_ja": "NHK",
        "lang": "ja",
    },
    "yahoo_jp": {
        "url": "https://news.yahoo.co.jp/rss/topics/business.xml",
        "name_ja": "Yahoo Japan",
        "lang": "ja",
    },
    "cnbc_rss": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
        "name_ja": "CNBC RSS",
        "lang": "en",
    },
    "reuters_jp": {
        "url": "https://assets.wor.jp/rss/rdf/reuters/top.rdf",
        "name_ja": "Reuters JP",
        "lang": "ja",
    },
    "bloomberg_rss": {
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "name_ja": "Bloomberg",
        "lang": "en",
    },
    "nikkei_rss": {
        "url": "https://assets.wor.jp/rss/rdf/nikkei/news.rdf",
        "name_ja": "日経RSS",
        "lang": "ja",
    },
}

# Keywords to filter relevant news
JAPAN_KEYWORDS = [
    "japan", "nikkei", "tokyo", "yen", "jpy", "boj", "bank of japan",
    "日経", "東京", "円", "日銀", "株", "市場", "為替", "東証",
    "topix", "sony", "toyota", "softbank", "semiconductor", "半導体",
]

MARKET_KEYWORDS = [
    "oil", "crude", "opec", "iran", "war", "conflict", "sanctions",
    "fed", "inflation", "cpi", "rate", "tariff", "trade", "trump",
    "china", "recession", "gdp", "employment", "gold", "rally", "crash",
    "sell-off", "selloff", "correction", "bear", "bull", "geopolitical",
    "原油", "戦争", "制裁", "関税", "インフレ", "利上げ", "利下げ",
    "地政学", "紛争", "中東", "景気", "暴落", "急落", "急騰",
]


class NewsCollector:
    """Hybrid news collector: Scrapling for rich sites + RSS for reliable coverage."""

    def __init__(self, timeout: int = 15):
        self.timeout = timeout

    # --- Scrapling scraping ---
    def _scrape_source(self, name: str, config: dict) -> list[dict[str, str]]:
        """Scrape a news site using Scrapling."""
        articles = []
        try:
            from scrapling import Fetcher
            fetcher = Fetcher(auto_match=False)
            page = fetcher.get(config["url"], timeout=self.timeout)

            found = []
            for selector in config["selectors"]:
                elements = page.css(selector)
                if elements:
                    found = elements
                    break

            for el in found[:25]:
                title = el.text.strip() if el.text else ""
                href = el.attrib.get("href", "")

                if not title or len(title) < 12:
                    continue
                if title.lower() in ("read more", "see more", "もっと見る", "more"):
                    continue

                # Resolve relative URLs
                if href and not href.startswith("http"):
                    base = config["url"].split("//")[0] + "//" + config["url"].split("//")[1].split("/")[0]
                    href = base + (href if href.startswith("/") else "/" + href)

                articles.append({
                    "title": title[:200],
                    "description": "",
                    "link": href,
                    "source": name,
                    "source_name_ja": config["name_ja"],
                    "lang": config["lang"],
                    "method": "scrape",
                })
        except Exception as e:
            logger.warning(f"Scrape failed for {name}: {e}")

        return articles

    # --- RSS fetching ---
    def _fetch_rss(self, name: str, config: dict) -> list[dict[str, str]]:
        """Fetch articles from an RSS feed."""
        articles = []
        try:
            resp = requests.get(config["url"], timeout=self.timeout, headers={
                "User-Agent": "Nikkei225-Agent/2.0"
            })
            resp.raise_for_status()
            root = ET.fromstring(resp.content)

            # Handle RSS 2.0 and RDF/RSS 1.0
            items = root.findall(".//item")
            if not items:
                ns = {"rss": "http://purl.org/rss/1.0/"}
                items = root.findall(".//rss:item", ns)

            for item in items[:20]:
                title = self._xml_text(item, "title")
                desc = self._xml_text(item, "description")
                link = self._xml_text(item, "link")

                if title and len(title.strip()) > 5:
                    articles.append({
                        "title": title.strip()[:200],
                        "description": (desc or "").strip()[:300],
                        "link": (link or "").strip(),
                        "source": name,
                        "source_name_ja": config["name_ja"],
                        "lang": config["lang"],
                        "method": "rss",
                    })
        except Exception as e:
            logger.warning(f"RSS failed for {name}: {e}")

        return articles

    def _xml_text(self, element, tag: str) -> str | None:
        """Get text from XML element, trying multiple namespace patterns."""
        for prefix in ["", "{http://purl.org/rss/1.0/}", "{http://purl.org/dc/elements/1.1/}"]:
            el = element.find(f"{prefix}{tag}")
            if el is not None and el.text:
                return el.text
        return None

    # --- Main collection ---
    def collect_all(self) -> dict[str, Any]:
        """Collect news from all sources (scraping + RSS)."""
        all_articles: list[dict[str, str]] = []
        sources_status: dict[str, str] = {}

        # Scrapling sources
        for name, config in SCRAPE_SOURCES.items():
            articles = self._scrape_source(name, config)
            all_articles.extend(articles)
            status = f"ok ({len(articles)})" if articles else "failed"
            sources_status[name] = status
            if articles:
                logger.info(f"  {name} [scrape]: {len(articles)} articles")
            else:
                logger.warning(f"  {name} [scrape]: failed")

        # RSS sources
        for name, config in RSS_SOURCES.items():
            articles = self._fetch_rss(name, config)
            all_articles.extend(articles)
            status = f"ok ({len(articles)})" if articles else "failed"
            sources_status[name] = status
            if articles:
                logger.info(f"  {name} [rss]: {len(articles)} articles")
            else:
                logger.warning(f"  {name} [rss]: failed")

        # Filter
        japan_related = self._filter_articles(all_articles, JAPAN_KEYWORDS)
        market_related = self._filter_articles(all_articles, MARKET_KEYWORDS)

        japan_related = self._deduplicate(japan_related)
        market_related = self._deduplicate(market_related)
        all_relevant = self._merge_and_rank(japan_related, market_related)

        return {
            "timestamp": datetime.now().isoformat(),
            "total_fetched": len(all_articles),
            "japan_related": japan_related[:15],
            "market_related": market_related[:15],
            "top_stories": all_relevant[:15],
            "sources_status": sources_status,
        }

    def _filter_articles(self, articles: list[dict], keywords: list[str]) -> list[dict]:
        result = []
        for article in articles:
            text = f"{article['title']} {article.get('description', '')}".lower()
            matched = [kw for kw in keywords if kw.lower() in text]
            if matched:
                article_copy = article.copy()
                article_copy["matched_keywords"] = matched
                result.append(article_copy)
        return result

    def _deduplicate(self, articles: list[dict]) -> list[dict]:
        seen_titles: list[str] = []
        unique = []
        for article in articles:
            title = article["title"].lower()
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
        seen = set()
        all_articles = []
        for a in japan:
            key = a["title"][:50]
            if key not in seen:
                a["relevance"] = "japan"
                a["score"] = len(a.get("matched_keywords", [])) * 2
                all_articles.append(a)
                seen.add(key)
        for a in market:
            key = a["title"][:50]
            if key not in seen:
                a["relevance"] = "global_market"
                a["score"] = len(a.get("matched_keywords", []))
                all_articles.append(a)
                seen.add(key)
        all_articles.sort(key=lambda x: x.get("score", 0), reverse=True)
        return all_articles
