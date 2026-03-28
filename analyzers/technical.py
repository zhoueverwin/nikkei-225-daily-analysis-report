"""Technical analysis module - calculates indicators from OHLCV data."""

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class TechnicalAnalyzer:
    """Computes technical indicators for market analysis."""

    def analyze(self, df: pd.DataFrame) -> dict[str, Any] | None:
        """Run full technical analysis on OHLCV DataFrame.

        Args:
            df: DataFrame with columns Open, High, Low, Close, Volume

        Returns:
            Dict with all computed indicators and signals.
        """
        if df is None or df.empty or len(df) < 26:
            logger.warning("Insufficient data for technical analysis")
            return None

        try:
            result = {
                "moving_averages": self._moving_averages(df),
                "rsi": self._rsi(df),
                "macd": self._macd(df),
                "bollinger_bands": self._bollinger_bands(df),
                "support_resistance": self._support_resistance(df),
                "volume_analysis": self._volume_analysis(df),
                "signals": [],
            }
            result["signals"] = self._generate_signals(result, df)
            return result
        except Exception as e:
            logger.error(f"Technical analysis failed: {e}")
            return None

    def _moving_averages(self, df: pd.DataFrame) -> dict[str, Any]:
        """Calculate moving averages (5, 25, 75 day - Japanese market standard)."""
        close = df["Close"]
        ma5 = close.rolling(5).mean()
        ma25 = close.rolling(25).mean()
        ma75 = close.rolling(75).mean() if len(close) >= 75 else pd.Series(dtype=float)

        current_price = float(close.iloc[-1])
        result = {
            "ma5": round(float(ma5.iloc[-1]), 2) if not ma5.empty and not pd.isna(ma5.iloc[-1]) else None,
            "ma25": round(float(ma25.iloc[-1]), 2) if not ma25.empty and not pd.isna(ma25.iloc[-1]) else None,
            "ma75": round(float(ma75.iloc[-1]), 2) if not ma75.empty and not pd.isna(ma75.iloc[-1]) else None,
            "price_vs_ma5": None,
            "price_vs_ma25": None,
            "golden_cross": False,
            "death_cross": False,
        }

        if result["ma5"]:
            result["price_vs_ma5"] = "above" if current_price > result["ma5"] else "below"
        if result["ma25"]:
            result["price_vs_ma25"] = "above" if current_price > result["ma25"] else "below"

        # Golden/Death cross detection (MA5 crossing MA25)
        if len(ma5) >= 2 and len(ma25) >= 2:
            prev_ma5 = ma5.iloc[-2]
            prev_ma25 = ma25.iloc[-2]
            curr_ma5 = ma5.iloc[-1]
            curr_ma25 = ma25.iloc[-1]
            if not any(pd.isna([prev_ma5, prev_ma25, curr_ma5, curr_ma25])):
                if prev_ma5 <= prev_ma25 and curr_ma5 > curr_ma25:
                    result["golden_cross"] = True
                elif prev_ma5 >= prev_ma25 and curr_ma5 < curr_ma25:
                    result["death_cross"] = True

        return result

    def _rsi(self, df: pd.DataFrame, period: int = 14) -> dict[str, Any]:
        """Calculate RSI (Relative Strength Index)."""
        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)

        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else None

        zone = "neutral"
        if current_rsi is not None:
            if current_rsi >= 70:
                zone = "overbought"
            elif current_rsi <= 30:
                zone = "oversold"

        return {
            "value": round(current_rsi, 2) if current_rsi else None,
            "zone": zone,
            "period": period,
        }

    def _macd(self, df: pd.DataFrame) -> dict[str, Any]:
        """Calculate MACD (Moving Average Convergence Divergence)."""
        close = df["Close"]
        ema12 = close.ewm(span=12).mean()
        ema26 = close.ewm(span=26).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9).mean()
        histogram = macd_line - signal_line

        current_macd = float(macd_line.iloc[-1])
        current_signal = float(signal_line.iloc[-1])
        current_hist = float(histogram.iloc[-1])

        # Crossover detection
        crossover = "none"
        if len(histogram) >= 2:
            prev_hist = float(histogram.iloc[-2])
            if prev_hist <= 0 and current_hist > 0:
                crossover = "bullish"
            elif prev_hist >= 0 and current_hist < 0:
                crossover = "bearish"

        return {
            "macd": round(current_macd, 2),
            "signal": round(current_signal, 2),
            "histogram": round(current_hist, 2),
            "crossover": crossover,
        }

    def _bollinger_bands(self, df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> dict[str, Any]:
        """Calculate Bollinger Bands."""
        close = df["Close"]
        sma = close.rolling(period).mean()
        std = close.rolling(period).std()

        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)

        current_price = float(close.iloc[-1])
        current_upper = float(upper.iloc[-1])
        current_lower = float(lower.iloc[-1])
        current_sma = float(sma.iloc[-1])

        band_width = (current_upper - current_lower) / current_sma * 100

        position = "middle"
        if current_price >= current_upper:
            position = "above_upper"
        elif current_price <= current_lower:
            position = "below_lower"
        elif current_price > current_sma:
            position = "upper_half"
        else:
            position = "lower_half"

        return {
            "upper": round(current_upper, 2),
            "middle": round(current_sma, 2),
            "lower": round(current_lower, 2),
            "band_width_pct": round(band_width, 2),
            "position": position,
        }

    def _support_resistance(self, df: pd.DataFrame, lookback: int = 20) -> dict[str, Any]:
        """Estimate support and resistance levels from recent price action."""
        recent = df.tail(lookback)
        highs = recent["High"]
        lows = recent["Low"]
        close = float(df["Close"].iloc[-1])

        resistance = float(highs.max())
        support = float(lows.min())

        # Secondary levels from recent pivots
        pivot = (resistance + support + close) / 3
        r1 = 2 * pivot - support
        s1 = 2 * pivot - resistance

        return {
            "resistance": round(resistance, 2),
            "support": round(support, 2),
            "pivot": round(pivot, 2),
            "r1": round(r1, 2),
            "s1": round(s1, 2),
        }

    def _volume_analysis(self, df: pd.DataFrame) -> dict[str, Any]:
        """Analyze volume patterns."""
        vol = df["Volume"]
        avg_vol_20 = float(vol.tail(20).mean())
        current_vol = float(vol.iloc[-1])
        vol_ratio = current_vol / avg_vol_20 if avg_vol_20 > 0 else 1.0

        trend = "normal"
        if vol_ratio > 1.5:
            trend = "high_volume"
        elif vol_ratio < 0.5:
            trend = "low_volume"

        return {
            "current": int(current_vol),
            "avg_20d": int(avg_vol_20),
            "ratio": round(vol_ratio, 2),
            "trend": trend,
        }

    def _generate_signals(self, indicators: dict, df: pd.DataFrame) -> list[dict[str, str]]:
        """Generate trading signals based on computed indicators."""
        signals = []

        # RSI signals
        rsi = indicators["rsi"]
        if rsi["zone"] == "overbought":
            signals.append({"type": "bearish", "source": "RSI", "detail": f"RSI {rsi['value']} - 買われすぎゾーン"})
        elif rsi["zone"] == "oversold":
            signals.append({"type": "bullish", "source": "RSI", "detail": f"RSI {rsi['value']} - 売られすぎゾーン"})

        # MACD crossover
        macd = indicators["macd"]
        if macd["crossover"] == "bullish":
            signals.append({"type": "bullish", "source": "MACD", "detail": "MACDゴールデンクロス"})
        elif macd["crossover"] == "bearish":
            signals.append({"type": "bearish", "source": "MACD", "detail": "MACDデッドクロス"})

        # MA crossover
        ma = indicators["moving_averages"]
        if ma["golden_cross"]:
            signals.append({"type": "bullish", "source": "MA", "detail": "5日線が25日線をゴールデンクロス"})
        elif ma["death_cross"]:
            signals.append({"type": "bearish", "source": "MA", "detail": "5日線が25日線をデッドクロス"})

        # Bollinger Band extremes
        bb = indicators["bollinger_bands"]
        if bb["position"] == "above_upper":
            signals.append({"type": "bearish", "source": "BB", "detail": "ボリンジャーバンド上限突破 - 反落注意"})
        elif bb["position"] == "below_lower":
            signals.append({"type": "bullish", "source": "BB", "detail": "ボリンジャーバンド下限突破 - 反発期待"})

        # Volume
        vol = indicators["volume_analysis"]
        if vol["trend"] == "high_volume":
            signals.append({"type": "info", "source": "Volume", "detail": f"出来高急増 (平均の{vol['ratio']}倍)"})

        return signals
