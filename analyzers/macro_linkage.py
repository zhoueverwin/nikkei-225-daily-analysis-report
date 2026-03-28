"""Macro linkage analyzer - evaluates cross-market relationships."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class MacroLinkageAnalyzer:
    """Analyzes macro-economic linkages affecting Nikkei 225."""

    def analyze(self, market_data: dict, macro_data: dict) -> dict[str, Any]:
        """Analyze macro linkages from collected data.

        Returns structured analysis of key macro relationships.
        """
        result: dict[str, Any] = {
            "fx_impact": self._analyze_fx_impact(market_data),
            "us_market_impact": self._analyze_us_impact(market_data),
            "commodity_impact": self._analyze_commodity_impact(market_data),
            "bond_signals": self._analyze_bonds(market_data, macro_data),
            "risk_appetite": self._assess_risk_appetite(market_data),
            "key_drivers": [],
        }
        result["key_drivers"] = self._identify_key_drivers(result, market_data)
        return result

    def _analyze_fx_impact(self, data: dict) -> dict[str, Any]:
        """Analyze FX impact on Japanese equities."""
        fx = data.get("fx", {})
        usdjpy = fx.get("usdjpy", {})

        if not usdjpy:
            return {"available": False}

        change_pct = usdjpy.get("change_pct", 0)
        rate = usdjpy.get("close", 0)

        # Yen weakening = generally positive for exporters (Nikkei)
        impact = "neutral"
        if change_pct > 0.3:
            impact = "positive"  # Yen weakening
        elif change_pct < -0.3:
            impact = "negative"  # Yen strengthening

        return {
            "available": True,
            "usdjpy_rate": rate,
            "usdjpy_change_pct": round(change_pct, 2),
            "impact_on_nikkei": impact,
            "explanation_ja": self._fx_explanation_ja(change_pct, rate),
        }

    def _fx_explanation_ja(self, change_pct: float, rate: float) -> str:
        if change_pct > 0.3:
            return f"ドル円 {rate:.2f} (前日比+{change_pct:.2f}%) — 円安進行は輸出企業の収益押し上げ要因"
        elif change_pct < -0.3:
            return f"ドル円 {rate:.2f} (前日比{change_pct:.2f}%) — 円高進行は輸出企業にとって逆風"
        else:
            return f"ドル円 {rate:.2f} (前日比{change_pct:+.2f}%) — 為替は小動き、影響は限定的"

    def _analyze_us_impact(self, data: dict) -> dict[str, Any]:
        """Analyze US market impact on Nikkei."""
        markets = data.get("market_data", {})
        sp500 = markets.get("sp500", {})
        nasdaq = markets.get("nasdaq", {})
        dow = markets.get("dow", {})
        vix = markets.get("vix", {})

        if not sp500:
            return {"available": False}

        sp_change = sp500.get("change_pct", 0)

        # Strong correlation between US and JP markets
        impact = "neutral"
        if sp_change > 0.5:
            impact = "positive"
        elif sp_change < -0.5:
            impact = "negative"

        return {
            "available": True,
            "sp500_change_pct": round(sp_change, 2),
            "nasdaq_change_pct": round(nasdaq.get("change_pct", 0), 2) if nasdaq else None,
            "dow_change_pct": round(dow.get("change_pct", 0), 2) if dow else None,
            "vix": round(vix.get("close", 0), 2) if vix else None,
            "vix_level": self._vix_level(vix.get("close", 0) if vix else 0),
            "impact_on_nikkei": impact,
        }

    def _vix_level(self, vix: float) -> str:
        if vix < 15:
            return "low_fear"
        elif vix < 25:
            return "moderate"
        elif vix < 35:
            return "elevated"
        else:
            return "extreme_fear"

    def _analyze_commodity_impact(self, data: dict) -> dict[str, Any]:
        """Analyze commodity price impacts."""
        commodities = data.get("commodities", {})
        oil = commodities.get("wti_oil", {})
        gold = commodities.get("gold", {})

        return {
            "available": bool(oil or gold),
            "oil_price": round(oil.get("close", 0), 2) if oil else None,
            "oil_change_pct": round(oil.get("change_pct", 0), 2) if oil else None,
            "gold_price": round(gold.get("close", 0), 2) if gold else None,
            "gold_change_pct": round(gold.get("change_pct", 0), 2) if gold else None,
        }

    def _analyze_bonds(self, market_data: dict, macro_data: dict) -> dict[str, Any]:
        """Analyze bond yield signals."""
        bonds_market = market_data.get("bonds", {})
        bonds_macro = macro_data.get("bonds", {})

        us_10y = bonds_market.get("us_10y", {})
        jp_10y = bonds_macro.get("jp_10y", {})

        result: dict[str, Any] = {"available": False}

        if us_10y:
            result["available"] = True
            result["us_10y_yield"] = round(us_10y.get("close", 0), 3)
            result["us_10y_change"] = round(us_10y.get("change", 0), 3)

        if jp_10y:
            result["available"] = True
            result["jp_10y_yield"] = jp_10y.get("latest_value")
            result["jp_10y_date"] = jp_10y.get("latest_date")

        # Yield spread
        if us_10y and jp_10y and jp_10y.get("latest_value"):
            spread = us_10y.get("close", 0) - jp_10y["latest_value"]
            result["us_jp_spread"] = round(spread, 3)

        return result

    def _assess_risk_appetite(self, data: dict) -> dict[str, str]:
        """Overall risk appetite assessment."""
        markets = data.get("market_data", {})
        vix_data = markets.get("vix", {})
        gold_data = data.get("commodities", {}).get("gold", {})

        vix = vix_data.get("close", 20) if vix_data else 20
        gold_change = gold_data.get("change_pct", 0) if gold_data else 0

        if vix > 30 or gold_change > 2:
            appetite = "risk_off"
            description_ja = "リスクオフムード — VIX高水準、安全資産選好"
        elif vix < 15 and gold_change < -0.5:
            appetite = "risk_on"
            description_ja = "リスクオンムード — VIX低水準、株式選好"
        else:
            appetite = "neutral"
            description_ja = "リスク選好は中立的"

        return {
            "appetite": appetite,
            "description_ja": description_ja,
        }

    def _identify_key_drivers(self, analysis: dict, market_data: dict) -> list[dict[str, str]]:
        """Identify the 2-3 most important market drivers today."""
        drivers = []

        # FX as driver
        fx = analysis["fx_impact"]
        if fx.get("available") and abs(fx.get("usdjpy_change_pct", 0)) > 0.5:
            drivers.append({
                "factor": "為替",
                "direction": "positive" if fx["usdjpy_change_pct"] > 0 else "negative",
                "importance": "high",
                "detail_ja": fx["explanation_ja"],
            })

        # US markets
        us = analysis["us_market_impact"]
        if us.get("available") and abs(us.get("sp500_change_pct", 0)) > 0.5:
            direction = "positive" if us["sp500_change_pct"] > 0 else "negative"
            drivers.append({
                "factor": "米国市場",
                "direction": direction,
                "importance": "high",
                "detail_ja": f"S&P500 {us['sp500_change_pct']:+.2f}% — 米国市場の動向が日本市場に波及",
            })

        # VIX spike
        if us.get("available") and us.get("vix_level") in ("elevated", "extreme_fear"):
            drivers.append({
                "factor": "VIX",
                "direction": "negative",
                "importance": "high",
                "detail_ja": f"VIX {us.get('vix', 'N/A')} — 恐怖指数上昇でリスクオフ圧力",
            })

        # Sort by importance
        return sorted(drivers, key=lambda d: d["importance"] == "high", reverse=True)[:3]
