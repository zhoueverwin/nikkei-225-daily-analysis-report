"""Market data collector using yfinance."""

import logging
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import yfinance as yf
import yaml

logger = logging.getLogger(__name__)


class MarketDataCollector:
    """Collects market data from Yahoo Finance."""

    def __init__(self, config_path: str = "config/data_sources.yaml"):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

    def fetch_ticker(self, symbol: str, period: str = "3mo") -> pd.DataFrame | None:
        """Fetch OHLCV data for a single ticker."""
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period)
            if df.empty:
                logger.warning(f"No data returned for {symbol}")
                return None
            return df
        except Exception as e:
            logger.error(f"Failed to fetch {symbol}: {e}")
            return None

    def fetch_current_price(self, symbol: str) -> dict[str, Any] | None:
        """Fetch current/latest price info for a ticker."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            hist = ticker.history(period="2d")
            if hist.empty:
                return None

            latest = hist.iloc[-1]
            prev = hist.iloc[-2] if len(hist) > 1 else latest

            return {
                "symbol": symbol,
                "close": float(latest["Close"]),
                "open": float(latest["Open"]),
                "high": float(latest["High"]),
                "low": float(latest["Low"]),
                "volume": int(latest["Volume"]),
                "prev_close": float(prev["Close"]),
                "change": float(latest["Close"] - prev["Close"]),
                "change_pct": float((latest["Close"] - prev["Close"]) / prev["Close"] * 100),
                "date": str(hist.index[-1].date()),
            }
        except Exception as e:
            logger.error(f"Failed to fetch current price for {symbol}: {e}")
            return None

    def collect_all(self) -> dict[str, Any]:
        """Collect all market data as defined in config."""
        result: dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "market_data": {},
            "fx": {},
            "commodities": {},
            "bonds": {},
            "errors": [],
        }

        # Market indices
        for name, cfg in self.config.get("market_data", {}).items():
            data = self.fetch_current_price(cfg["symbol"])
            if data:
                result["market_data"][name] = data
            else:
                result["errors"].append(f"market_data.{name}: failed")
                logger.warning(f"Failed to collect {name}, trying fallback...")
                if "fallback_symbol" in cfg:
                    data = self.fetch_current_price(cfg["fallback_symbol"])
                    if data:
                        result["market_data"][name] = data
                        result["market_data"][name]["source"] = "fallback"

        # FX
        for name, cfg in self.config.get("fx", {}).items():
            data = self.fetch_current_price(cfg["symbol"])
            if data:
                result["fx"][name] = data
            else:
                result["errors"].append(f"fx.{name}: failed")

        # Commodities
        for name, cfg in self.config.get("commodities", {}).items():
            data = self.fetch_current_price(cfg["symbol"])
            if data:
                result["commodities"][name] = data
            else:
                result["errors"].append(f"commodities.{name}: failed")

        # Bonds (yfinance ones)
        for name, cfg in self.config.get("bonds", {}).items():
            if cfg.get("source") == "yfinance":
                data = self.fetch_current_price(cfg["symbol"])
                if data:
                    result["bonds"][name] = data
                else:
                    result["errors"].append(f"bonds.{name}: failed")

        return result

    def fetch_historical(self, symbol: str, period: str = "3mo") -> pd.DataFrame | None:
        """Fetch historical OHLCV for technical analysis."""
        return self.fetch_ticker(symbol, period)
