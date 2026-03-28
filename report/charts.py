"""Chart data generation for HTML report."""

from typing import Any

import pandas as pd


def prepare_candlestick_data(df: pd.DataFrame, days: int = 30) -> list[dict[str, Any]]:
    """Prepare OHLCV data for Lightweight Charts candlestick rendering."""
    if df is None or df.empty:
        return []

    recent = df.tail(days)
    data = []
    for idx, row in recent.iterrows():
        data.append({
            "time": str(idx.date()) if hasattr(idx, "date") else str(idx),
            "open": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "close": round(float(row["Close"]), 2),
        })
    return data


def prepare_volume_data(df: pd.DataFrame, days: int = 30) -> list[dict[str, Any]]:
    """Prepare volume data for chart rendering."""
    if df is None or df.empty:
        return []

    recent = df.tail(days)
    data = []
    for idx, row in recent.iterrows():
        color = "rgba(108, 255, 168, 0.5)" if row["Close"] >= row["Open"] else "rgba(255, 108, 140, 0.5)"
        data.append({
            "time": str(idx.date()) if hasattr(idx, "date") else str(idx),
            "value": int(row["Volume"]),
            "color": color,
        })
    return data


def prepare_line_data(series: pd.Series, days: int = 30) -> list[dict[str, Any]]:
    """Prepare a simple line series for chart rendering."""
    if series is None or series.empty:
        return []

    recent = series.tail(days).dropna()
    return [
        {"time": str(idx.date()) if hasattr(idx, "date") else str(idx), "value": round(float(v), 2)}
        for idx, v in recent.items()
    ]
