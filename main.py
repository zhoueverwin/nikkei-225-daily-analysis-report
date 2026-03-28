"""Nikkei 225 Daily Analysis Agent - Main Pipeline Orchestrator.

Runs the full analysis pipeline:
  1. Collect market & macro data
  2. Run technical analysis
  3. Run macro linkage analysis
  4. Load memory, detect contradictions, verify predictions
  5. Generate LLM analysis
  6. Generate HTML report
  7. Save daily memory record
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

from analyzers.macro_linkage import MacroLinkageAnalyzer
from analyzers.technical import TechnicalAnalyzer
from collectors.fallback import assess_data_quality
from collectors.macro_data import MacroDataCollector
from collectors.market_data import MarketDataCollector
from llm.engine import LLMEngine
from memory.contradiction import ContradictionDetector
from memory.memory_manager import MemoryManager
from memory.prediction_tracker import PredictionTracker
from report.generator import ReportGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("nikkei-agent")


def load_config(path: str = "config/settings.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def run_pipeline(config: dict, dry_run: bool = False) -> Path | None:
    """Execute the full analysis pipeline.

    Args:
        config: Settings dict from settings.yaml
        dry_run: If True, skip LLM calls and use placeholder analysis

    Returns:
        Path to the generated report, or None on failure.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"=== Nikkei 225 Daily Analysis Pipeline - {today} ===")

    # --- Step 1: Data Collection ---
    logger.info("Step 1/7: Collecting market data...")
    market_collector = MarketDataCollector()
    market_data = market_collector.collect_all()
    logger.info(
        f"  Market data: {len(market_data.get('market_data', {}))} indices, "
        f"{len(market_data.get('fx', {}))} FX, "
        f"{len(market_data.get('commodities', {}))} commodities"
    )

    logger.info("Step 1/7: Collecting macro data...")
    macro_collector = MacroDataCollector()
    macro_data = macro_collector.collect_all()
    logger.info(f"  Macro data: {len(macro_data.get('macro', {}))} series")

    # Data quality check
    quality = assess_data_quality(market_data, macro_data)
    quality_dict = quality.to_dict()
    logger.info(f"  Data completeness: {quality_dict['completeness_pct']}%")
    if not quality.is_sufficient:
        logger.warning("  Data completeness below 80% - report quality may be degraded")

    # --- Step 2: Technical Analysis ---
    logger.info("Step 2/7: Running technical analysis...")
    tech_analyzer = TechnicalAnalyzer()
    nikkei_hist = market_collector.fetch_historical("^N225", period="3mo")
    technical = tech_analyzer.analyze(nikkei_hist)
    if technical:
        signals = technical.get("signals", [])
        logger.info(f"  Technical signals: {len(signals)}")
    else:
        logger.warning("  Technical analysis unavailable (insufficient data)")

    # --- Step 3: Macro Linkage Analysis ---
    logger.info("Step 3/7: Analyzing macro linkages...")
    macro_analyzer = MacroLinkageAnalyzer()
    macro_linkage = macro_analyzer.analyze(market_data, macro_data)
    drivers = macro_linkage.get("key_drivers", [])
    logger.info(f"  Key drivers identified: {len(drivers)}")

    # --- Step 4: Memory & Contradiction Detection ---
    logger.info("Step 4/7: Loading memory and checking contradictions...")
    mem_config = config.get("memory", {})
    memory = MemoryManager(
        store_dir=mem_config.get("store_dir", "memory/store"),
        short_term_days=mem_config.get("short_term_days", 7),
        medium_term_days=mem_config.get("medium_term_days", 30),
    )

    recent_memory = memory.load_recent()
    stance_history = memory.get_stance_history()
    open_observations = memory.get_open_observations()
    logger.info(f"  Loaded {len(recent_memory)} days of memory")

    # Contradiction detection
    detector = ContradictionDetector()
    contradiction_flags = detector.detect(market_data, stance_history, open_observations)
    if contradiction_flags:
        logger.info(f"  Contradictions detected: {len(contradiction_flags)}")

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

    # Historical accuracy
    pred_history = memory.get_prediction_history(days=30)
    if pred_history:
        verifications = []
        for ph in pred_history:
            if "result" in ph:
                verifications.append(ph)
        if verifications:
            prediction_accuracy = tracker.calculate_accuracy(verifications)

    memory_context = {
        "recent_days": len(recent_memory),
        "stance_history": stance_history[-3:] if stance_history else [],
        "open_observations": open_observations,
    }

    # --- Step 5: LLM Analysis ---
    logger.info("Step 5/7: Generating LLM analysis...")
    if dry_run:
        logger.info("  (dry run - using placeholder analysis)")
        nikkei = market_data.get("market_data", {}).get("nikkei", {})
        llm_analysis = {
            "headline_ja": f"日経225: {nikkei.get('close', 'N/A')} ({nikkei.get('change_pct', 0):+.2f}%)",
            "summary_ja": "ドライランモードのため、LLM分析は省略されています。実行時には詳細な分析が生成されます。",
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
            "stance": {
                "direction": "neutral",
                "confidence": "low",
                "key_assumptions": [],
            },
            "open_observations": [],
            "risk_events": [],
            "beginner_lesson": {
                "topic_ja": "テクニカル分析とは？",
                "explanation_ja": "テクニカル分析とは、過去の価格や出来高のパターンから将来の値動きを予測する手法です。チャートを読む力を身につけることで、より良い投資判断ができるようになります。",
            },
        }
    else:
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

        # Generate beginner lesson
        if llm_analysis.get("beginner_topic_hint_ja") and not llm_analysis.get("beginner_lesson"):
            lesson = engine.generate_beginner_lesson(llm_analysis["beginner_topic_hint_ja"])
            llm_analysis["beginner_lesson"] = lesson

    # --- Step 6: Report Generation ---
    logger.info("Step 6/7: Generating HTML report...")
    report_config = config.get("report", {})
    generator = ReportGenerator(
        output_dir=report_config.get("output_dir", "docs"),
    )
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
    )
    logger.info(f"  Report saved: {report_path}")

    # --- Step 7: Save Memory ---
    logger.info("Step 7/7: Saving daily memory record...")
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
        sentiment_score=None,
        contradiction_flags=[f.get("detail_ja", "") for f in contradiction_flags],
        prediction_verification=prediction_verification,
    )
    memory.save_daily_record(record)

    # Cleanup old records
    memory.cleanup_old_records()

    logger.info(f"=== Pipeline complete. Report: {report_path} ===")
    return report_path


def main():
    parser = argparse.ArgumentParser(description="Nikkei 225 Daily Analysis Agent")
    parser.add_argument("--dry-run", action="store_true", help="Skip LLM calls, use placeholder analysis")
    parser.add_argument("--config", default="config/settings.yaml", help="Path to settings.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    report = run_pipeline(config, dry_run=args.dry_run)

    if report:
        print(f"\nReport generated: {report}")
    else:
        print("\nPipeline failed.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
