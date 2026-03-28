"""Nikkei 225 Daily Analysis Agent - Main Pipeline Orchestrator.

Runs the full analysis pipeline:
  1. Collect market, macro, news, and sector data
  2. Run technical analysis
  3. Run macro linkage + sector rotation + sentiment analysis
  4. Load memory, detect contradictions, verify predictions
  5. Generate LLM analysis (or accept injected analysis)
  6. Generate HTML report with interactive charts
  7. Save daily memory record + send notifications
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

from analyzers.macro_linkage import MacroLinkageAnalyzer
from analyzers.sector_rotation import SectorRotationAnalyzer
from analyzers.sentiment import SentimentAnalyzer
from analyzers.technical import TechnicalAnalyzer
from collectors.economic_calendar import EconomicCalendar
from collectors.fallback import assess_data_quality
from collectors.macro_data import MacroDataCollector
from collectors.market_data import MarketDataCollector
from collectors.news_collector import NewsCollector
from memory.beginner_topics import BeginnerTopicManager
from memory.contradiction import ContradictionDetector
from memory.memory_manager import MemoryManager
from memory.prediction_tracker import PredictionTracker
from report.charts import prepare_candlestick_data, prepare_volume_data
from report.generator import ReportGenerator
from report.index_generator import IndexGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("nikkei-agent")


def load_config(path: str = "config/settings.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def run_pipeline(
    config: dict,
    dry_run: bool = False,
    analysis_json: str | None = None,
) -> Path | None:
    """Execute the full analysis pipeline.

    Args:
        config: Settings dict from settings.yaml
        dry_run: If True, skip LLM calls and use placeholder analysis
        analysis_json: Path to a JSON file with pre-generated LLM analysis

    Returns:
        Path to the generated report, or None on failure.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"=== Nikkei 225 Daily Analysis Pipeline - {today} ===")

    # --- Step 1: Data Collection ---
    logger.info("Step 1/7: Collecting data...")

    # Market data
    market_collector = MarketDataCollector()
    market_data = market_collector.collect_all()
    logger.info(
        f"  Market: {len(market_data.get('market_data', {}))} indices, "
        f"{len(market_data.get('fx', {}))} FX, "
        f"{len(market_data.get('commodities', {}))} commodities"
    )

    # Macro data
    macro_collector = MacroDataCollector()
    macro_data = macro_collector.collect_all()
    logger.info(f"  Macro: {len(macro_data.get('macro', {}))} series")

    # News data (Phase 2)
    logger.info("  Collecting news...")
    news_collector = NewsCollector()
    news_data = news_collector.collect_all()
    logger.info(f"  News: {news_data.get('total_fetched', 0)} articles, {len(news_data.get('top_stories', []))} relevant")

    # Sector data (Phase 2)
    logger.info("  Collecting sector data...")
    sector_analyzer = SectorRotationAnalyzer()
    sector_data = sector_analyzer.analyze()
    if sector_data.get("available"):
        logger.info(f"  Sectors: {len(sector_data.get('sectors', {}))} tracked")
    else:
        logger.warning("  Sector data unavailable")

    # Economic calendar (Phase 2)
    calendar = EconomicCalendar()
    calendar_data = calendar.get_upcoming_events(days_ahead=14)
    logger.info(f"  Calendar: {calendar_data.get('total_events', 0)} upcoming events")

    # Data quality check
    quality = assess_data_quality(market_data, macro_data)
    quality_dict = quality.to_dict()
    logger.info(f"  Data completeness: {quality_dict['completeness_pct']}%")

    # --- Step 2: Technical Analysis ---
    logger.info("Step 2/7: Running technical analysis...")
    tech_analyzer = TechnicalAnalyzer()
    nikkei_hist = market_collector.fetch_historical("^N225", period="3mo")
    technical = tech_analyzer.analyze(nikkei_hist)
    if technical:
        logger.info(f"  Signals: {len(technical.get('signals', []))}")

    # Prepare chart data (Phase 3)
    candlestick_data = prepare_candlestick_data(nikkei_hist, days=60) if nikkei_hist is not None else None
    volume_data = prepare_volume_data(nikkei_hist, days=60) if nikkei_hist is not None else None

    # --- Step 3: Analysis ---
    logger.info("Step 3/7: Running analysis modules...")

    # Macro linkage
    macro_analyzer = MacroLinkageAnalyzer()
    macro_linkage = macro_analyzer.analyze(market_data, macro_data)
    logger.info(f"  Key drivers: {len(macro_linkage.get('key_drivers', []))}")

    # Sentiment analysis (Phase 2)
    sentiment_analyzer = SentimentAnalyzer()
    all_news = news_data.get("top_stories", []) + news_data.get("japan_related", []) + news_data.get("market_related", [])
    # Deduplicate by title
    seen = set()
    unique_news = []
    for a in all_news:
        if a["title"] not in seen:
            unique_news.append(a)
            seen.add(a["title"])
    sentiment_data = sentiment_analyzer.analyze_batch(unique_news)
    sentiment_data["summary_ja"] = sentiment_analyzer.generate_summary_ja(sentiment_data)
    logger.info(f"  Sentiment: {sentiment_data['overall_label']} ({sentiment_data['overall_score']:+.3f})")

    # --- Step 4: Memory & Contradiction Detection ---
    logger.info("Step 4/7: Loading memory...")
    mem_config = config.get("memory", {})
    memory = MemoryManager(
        store_dir=mem_config.get("store_dir", "memory/store"),
        short_term_days=mem_config.get("short_term_days", 7),
        medium_term_days=mem_config.get("medium_term_days", 30),
    )

    recent_memory = memory.load_recent()
    stance_history = memory.get_stance_history()
    open_observations = memory.get_open_observations()
    logger.info(f"  Memory: {len(recent_memory)} days loaded")

    detector = ContradictionDetector()
    contradiction_flags = detector.detect(market_data, stance_history, open_observations)
    if contradiction_flags:
        logger.info(f"  Contradictions: {len(contradiction_flags)}")

    # Prediction verification
    tracker = PredictionTracker()
    prediction_verification = None
    prediction_accuracy = None

    if recent_memory:
        latest = recent_memory[-1]
        for pred in latest.get("predictions", []):
            prediction_verification = tracker.verify_prediction(pred, market_data)
            if prediction_verification:
                logger.info(f"  Previous prediction: {prediction_verification['result']}")
                break

    pred_history = memory.get_prediction_history(days=30)
    if pred_history:
        verifications = [ph for ph in pred_history if "result" in ph]
        if verifications:
            prediction_accuracy = tracker.calculate_accuracy(verifications)

    memory_context = {
        "recent_days": len(recent_memory),
        "stance_history": stance_history[-3:] if stance_history else [],
        "open_observations": open_observations,
    }

    # --- Step 5: LLM Analysis ---
    logger.info("Step 5/7: Generating analysis...")

    if analysis_json:
        # Load pre-generated analysis from file
        with open(analysis_json, encoding="utf-8") as f:
            llm_analysis = json.load(f)
        logger.info(f"  Loaded analysis from {analysis_json}")
    elif dry_run:
        logger.info("  (dry run - placeholder analysis)")
        nikkei = market_data.get("market_data", {}).get("nikkei", {})
        drivers = macro_linkage.get("key_drivers", [])
        llm_analysis = {
            "headline_ja": f"日経225: {nikkei.get('close', 'N/A')} ({nikkei.get('change_pct', 0):+.2f}%)",
            "summary_ja": "ドライランモードのため、LLM分析は省略されています。",
            "drivers": [
                {
                    "factor_ja": d.get("factor", ""),
                    "impact": d.get("direction", "neutral"),
                    "importance": d.get("importance", "medium"),
                    "detail_ja": d.get("detail_ja", ""),
                }
                for d in drivers
            ],
            "technical_summary_ja": "テクニカル分析データは取得済みです。",
            "continuity_ja": "記憶システム稼働中。" if recent_memory else "初回実行のため過去データなし。",
            "prediction": {
                "direction": "flat",
                "confidence": "low",
                "reasoning_ja": "ドライランモード",
                "risk_factors_ja": [],
            },
            "stance": {"direction": "neutral", "confidence": "low", "key_assumptions": []},
            "open_observations": [],
            "risk_events": [],
            "beginner_lesson": None,
        }
    else:
        # Try LLM engine (requires ANTHROPIC_API_KEY)
        try:
            from llm.engine import LLMEngine
            llm_config = config.get("llm", {})
            engine = LLMEngine(
                model=llm_config.get("model", "claude-sonnet-4-6"),
                max_tokens=llm_config.get("max_tokens", 8192),
                temperature=llm_config.get("temperature", 0.3),
            )
            llm_analysis = engine.generate_analysis(
                market_data=market_data,
                macro_data=macro_data,
                technical_analysis=technical,
                macro_linkage=macro_linkage,
                memory_context=memory_context,
                contradiction_flags=contradiction_flags,
                prediction_verification=prediction_verification,
                data_quality=quality_dict,
            )
            if llm_analysis.get("beginner_topic_hint_ja") and not llm_analysis.get("beginner_lesson"):
                lesson = engine.generate_beginner_lesson(llm_analysis["beginner_topic_hint_ja"])
                llm_analysis["beginner_lesson"] = lesson
        except Exception as e:
            logger.warning(f"  LLM unavailable ({e}), using fallback")
            llm_analysis = {
                "headline_ja": f"日経225: データのみ表示",
                "summary_ja": "LLMが利用できないため、データのみの表示です。",
                "drivers": [],
                "prediction": {"direction": "unknown", "confidence": "low", "reasoning_ja": "分析エンジン停止中"},
                "stance": {"direction": "neutral", "confidence": "low", "key_assumptions": []},
                "open_observations": [],
                "risk_events": [],
            }

    # Beginner lesson from library (Phase 3)
    if not llm_analysis.get("beginner_lesson"):
        topic_mgr = BeginnerTopicManager()
        context_keywords = []
        if technical:
            if technical.get("rsi", {}).get("zone") != "neutral":
                context_keywords.append("rsi")
            if technical.get("macd", {}).get("crossover") != "none":
                context_keywords.append("macd")
            if technical.get("moving_averages", {}).get("golden_cross") or technical.get("moving_averages", {}).get("death_cross"):
                context_keywords.append("ゴールデンクロス")
        if macro_linkage.get("risk_appetite", {}).get("appetite") == "risk_off":
            context_keywords.extend(["vix", "リスクオフ"])
        if abs(market_data.get("fx", {}).get("usdjpy", {}).get("change_pct", 0)) > 0.5:
            context_keywords.append("円安")
        if abs(market_data.get("commodities", {}).get("wti_oil", {}).get("change_pct", 0)) > 3:
            context_keywords.append("原油")
        if sector_data and sector_data.get("available"):
            context_keywords.append("セクター")

        lesson = topic_mgr.select_topic(context_keywords or ["rsi"], today)
        if lesson:
            llm_analysis["beginner_lesson"] = lesson
            topic_mgr.mark_covered(lesson["topic_ja"], today)

    # --- Step 6: Report Generation ---
    logger.info("Step 6/7: Generating HTML report...")
    report_config = config.get("report", {})
    generator = ReportGenerator(output_dir=report_config.get("output_dir", "docs"))
    report_path = generator.generate(
        date=today,
        market_data=market_data,
        macro_data=macro_data,
        technical_analysis=technical,
        macro_linkage=macro_linkage,
        llm_analysis=llm_analysis,
        prediction_accuracy=prediction_accuracy,
        prediction_verification=prediction_verification,
        data_quality=quality_dict,
        contradiction_flags=contradiction_flags,
        news_data=news_data,
        sentiment_data=sentiment_data,
        sector_data=sector_data,
        calendar_data=calendar_data,
        candlestick_data=candlestick_data,
        volume_data=volume_data,
    )
    logger.info(f"  Report: {report_path}")

    # Generate archive index (Phase 3)
    index_gen = IndexGenerator(docs_dir=report_config.get("output_dir", "docs"))
    index_gen.generate()

    # --- Step 7: Save Memory & Notify ---
    logger.info("Step 7/7: Saving memory & notifications...")
    nikkei_data = market_data.get("market_data", {}).get("nikkei", {})
    usdjpy_data = market_data.get("fx", {}).get("usdjpy", {})
    sp500_data = market_data.get("market_data", {}).get("sp500", {})
    vix_data = market_data.get("market_data", {}).get("vix", {})
    oil_data = market_data.get("commodities", {}).get("wti_oil", {})

    market_snapshot = {
        "nikkei_close": nikkei_data.get("close"),
        "nikkei_change_pct": nikkei_data.get("change_pct"),
        "usdjpy": usdjpy_data.get("close"),
        "sp500_close": sp500_data.get("close"),
        "vix": vix_data.get("close"),
        "wti_oil": oil_data.get("close"),
    }

    record = memory.create_daily_record(
        date=today,
        market_snapshot=market_snapshot,
        core_conclusion=llm_analysis.get("summary_ja", ""),
        stance=llm_analysis.get("stance", {}),
        predictions=([llm_analysis["prediction"]] if llm_analysis.get("prediction") else []),
        open_observations=llm_analysis.get("open_observations", []),
        sentiment_score=sentiment_data.get("overall_score") if sentiment_data else None,
        contradiction_flags=[f.get("detail_ja", "") for f in contradiction_flags],
        prediction_verification=prediction_verification,
    )
    memory.save_daily_record(record)
    memory.cleanup_old_records()

    # Send notifications (Phase 3)
    try:
        from notifications.telegram import TelegramNotifier, LineNotifier
        report_url = config.get("report", {}).get("github_pages_url")

        tg = TelegramNotifier()
        if tg.is_configured:
            tg.send_report_summary(today, nikkei_data, llm_analysis, report_url)

        line = LineNotifier()
        if line.is_configured:
            line.send_report_summary(today, nikkei_data, llm_analysis, report_url)
    except Exception as e:
        logger.debug(f"Notifications skipped: {e}")

    logger.info(f"=== Pipeline complete. Report: {report_path} ===")
    return report_path


def main():
    parser = argparse.ArgumentParser(description="Nikkei 225 Daily Analysis Agent")
    parser.add_argument("--dry-run", action="store_true", help="Skip LLM calls, use placeholder analysis")
    parser.add_argument("--analysis", type=str, help="Path to pre-generated analysis JSON file")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to settings.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    report = run_pipeline(config, dry_run=args.dry_run, analysis_json=args.analysis)

    if report:
        print(f"\nReport generated: {report}")
    else:
        print("\nPipeline failed.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
