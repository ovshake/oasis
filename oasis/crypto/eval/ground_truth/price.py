"""Ground-truth price fetchers.

Binance public klines API for crypto 1m data.
yfinance for commodities at daily resolution.
All results cached to data/market/ground_truth/cache/.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path("data/market/ground_truth/cache")

# Binance base URL (public, no auth needed)
_BINANCE_KLINES = "https://api.binance.com/api/v3/klines"

_BINANCE_SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "USDT": "USDTUSD",
}

_YFINANCE_TICKERS = {
    "XAU": "GC=F",
    "WTI": "CL=F",
}


def _cache_key(assets: list[str], start: str, end: str, res: str) -> str:
    raw = f"{sorted(assets)}|{start}|{end}|{res}"
    return hashlib.md5(raw.encode()).hexdigest()


def fetch_real_prices(
    assets: list[str],
    start: str,
    end: str,
    resolution: str = "1m",
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Fetch historical prices for *assets* between *start* and *end*.

    Returns a DataFrame with columns: datetime, <asset1>, <asset2>, ...
    Resolution: '1m' for crypto via Binance, '1d' for commodities via yfinance.

    Results are cached to parquet for offline reproducibility.
    """
    cdir = cache_dir or _CACHE_DIR
    cdir.mkdir(parents=True, exist_ok=True)
    cache_path = cdir / f"prices_{_cache_key(assets, start, end, resolution)}.parquet"

    if cache_path.exists():
        logger.info("Loading cached prices from %s", cache_path)
        return pd.read_parquet(cache_path)

    frames: dict[str, pd.Series] = {}
    for asset in assets:
        if asset in _BINANCE_SYMBOLS and resolution == "1m":
            series = _fetch_binance(asset, start, end)
        elif asset in _YFINANCE_TICKERS:
            series = _fetch_yfinance(asset, start, end)
        else:
            logger.warning("No fetcher for asset=%s resolution=%s", asset, resolution)
            continue
        if series is not None and not series.empty:
            frames[asset] = series

    if not frames:
        return pd.DataFrame()

    df = pd.DataFrame(frames)
    df.index.name = "datetime"
    df = df.reset_index()

    # Cache
    try:
        df.to_parquet(cache_path, index=False)
    except Exception as e:
        logger.warning("Failed to cache prices: %s", e)

    return df


def _fetch_binance(asset: str, start: str, end: str) -> pd.Series | None:
    """Fetch 1m klines from Binance public API."""
    try:
        import requests
    except ImportError:
        logger.warning("requests not installed; cannot fetch Binance data")
        return None

    symbol = _BINANCE_SYMBOLS[asset]
    start_ms = int(datetime.fromisoformat(start).timestamp() * 1000)
    end_ms = int(datetime.fromisoformat(end).timestamp() * 1000)

    all_rows: list[list] = []
    current = start_ms
    while current < end_ms:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": current,
            "endTime": end_ms,
            "limit": 1000,
        }
        try:
            resp = requests.get(_BINANCE_KLINES, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("Binance fetch failed for %s: %s", asset, e)
            break
        if not data:
            break
        all_rows.extend(data)
        current = data[-1][0] + 60_000  # next minute

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows)
    # Binance kline: [open_time, open, high, low, close, volume, ...]
    times = pd.to_datetime(df[0], unit="ms")
    closes = df[4].astype(float)
    return pd.Series(closes.values, index=times, name=asset)


def _fetch_yfinance(asset: str, start: str, end: str) -> pd.Series | None:
    """Fetch daily prices via yfinance."""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance not installed; cannot fetch commodity data")
        return None

    ticker = _YFINANCE_TICKERS[asset]
    try:
        data = yf.download(ticker, start=start, end=end, progress=False)
        if data.empty:
            return None
        return data["Close"].rename(asset)
    except Exception as e:
        logger.warning("yfinance fetch failed for %s: %s", asset, e)
        return None
