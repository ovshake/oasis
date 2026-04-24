"""Live + historical price resolver with caching.

Phase 9 deliverable. Resolves initial asset prices for a scenario run via
a priority hierarchy:

    explicit initial_prices > as_of_date historical > live fetch > assets.yaml default

Sources:
- Crypto (BTC, ETH, USDT): Binance public klines API (1-min resolution).
- Commodities (XAU, WTI): yfinance GC=F / CL=F (DAILY resolution only).
- Stablecoins: snap to peg_target unless explicitly overridden.

Cache: ``data/market/price_cache/{date}.json`` for offline reproducibility.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


class PriceResolution(BaseModel):
    """Result of resolving a single asset's price."""

    asset: str
    price: float
    source: Literal["manual", "live", "historical", "default", "peg_snap"]
    resolution: Literal["minute", "daily"] = "minute"
    fetched_at: datetime


# ---------------------------------------------------------------------------
# Binance klines helper
# ---------------------------------------------------------------------------

_BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"


def binance_klines(
    symbol: str,
    interval: str = "1m",
    start_ms: int = 0,
    end_ms: int = 0,
    limit: int = 1000,
) -> list[dict]:
    """Fetch klines from the Binance public API (no auth required).

    Parameters
    ----------
    symbol : Binance symbol, e.g. ``"BTCUSDT"``.
    interval : Kline interval, e.g. ``"1m"``, ``"1h"``, ``"1d"``.
    start_ms : Start time in epoch milliseconds.
    end_ms : End time in epoch milliseconds.
    limit : Max klines to return (Binance caps at 1000).

    Returns
    -------
    list[dict] with keys: open_time, open, high, low, close, volume, close_time.
    """
    import requests

    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": interval,
        "limit": min(limit, 1000),
    }
    if start_ms:
        params["startTime"] = start_ms
    if end_ms:
        params["endTime"] = end_ms

    resp = requests.get(_BINANCE_KLINES_URL, params=params, timeout=15)
    resp.raise_for_status()
    raw: list[list] = resp.json()

    result: list[dict] = []
    for k in raw:
        result.append(
            {
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
            }
        )
    return result


# ---------------------------------------------------------------------------
# Asset metadata helpers
# ---------------------------------------------------------------------------


def _load_assets_yaml(path: Path) -> list[dict]:
    """Return the ``assets`` list from an assets YAML file."""
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data["assets"]


def _asset_lookup(assets: list[dict]) -> dict[str, dict]:
    """Build {symbol: asset_dict} from the assets list."""
    return {a["symbol"]: a for a in assets}


_CRYPTO_SYMBOLS = {"BTC", "ETH", "USDT"}
_COMMODITY_SYMBOLS = {"XAU", "WTI"}
_STABLECOIN_SYMBOLS = {"USDT"}


def _is_crypto(symbol: str) -> bool:
    return symbol in _CRYPTO_SYMBOLS


def _is_commodity(symbol: str) -> bool:
    return symbol in _COMMODITY_SYMBOLS


def _is_stablecoin(symbol: str) -> bool:
    return symbol in _STABLECOIN_SYMBOLS


def _symbol_to_binance(symbol: str) -> str | None:
    """Map our symbol to Binance pair (vs USDT)."""
    mapping = {"BTC": "BTCUSDT", "ETH": "ETHUSDT", "USDT": "USDTUSD"}
    return mapping.get(symbol)


def _symbol_to_yfinance(symbol: str, assets: dict[str, dict]) -> str | None:
    """Map our symbol to yfinance ticker via assets.yaml metadata."""
    asset = assets.get(symbol, {})
    return asset.get("yfinance_ticker")


# ---------------------------------------------------------------------------
# Cache layer
# ---------------------------------------------------------------------------

_DEFAULT_CACHE_DIR = Path("data/market/price_cache")


def _cache_path(cache_dir: Path, key: str) -> Path:
    return cache_dir / f"{key}.json"


def _read_cache(cache_dir: Path, key: str) -> dict[str, Any] | None:
    path = _cache_path(cache_dir, key)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(cache_dir: Path, key: str, data: dict[str, Any]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = _cache_path(cache_dir, key)
    path.write_text(json.dumps(data, default=str), encoding="utf-8")


# ---------------------------------------------------------------------------
# Live price fetchers
# ---------------------------------------------------------------------------


def _fetch_crypto_live(symbols: list[str]) -> dict[str, float]:
    """Fetch latest close from Binance for crypto symbols."""
    prices: dict[str, float] = {}
    for sym in symbols:
        if not _is_crypto(sym):
            continue
        bsym = _symbol_to_binance(sym)
        if bsym is None:
            continue
        try:
            klines = binance_klines(bsym, interval="1m", limit=1)
            if klines:
                prices[sym] = klines[-1]["close"]
        except Exception as exc:
            logger.warning("Binance live fetch failed for %s: %s", sym, exc)
    return prices


def _fetch_commodity_live(
    symbols: list[str], asset_lookup: dict[str, dict]
) -> dict[str, float]:
    """Fetch latest daily close from yfinance for commodity symbols."""
    prices: dict[str, float] = {}
    for sym in symbols:
        if not _is_commodity(sym):
            continue
        ticker_str = _symbol_to_yfinance(sym, asset_lookup)
        if ticker_str is None:
            continue
        try:
            import yfinance as yf

            ticker = yf.Ticker(ticker_str)
            hist = ticker.history(period="5d")
            if hist is not None and not hist.empty:
                prices[sym] = float(hist["Close"].iloc[-1])
        except Exception as exc:
            logger.warning("yfinance live fetch failed for %s: %s", sym, exc)
    return prices


def fetch_live_prices(
    symbols: list[str],
    assets_yaml_path: Path | None = None,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> dict[str, PriceResolution]:
    """Fetch current prices. Crypto via Binance; commodities via yfinance.

    Results are cached to ``data/market/price_cache/live_{YYYY-MM-DD}.json``.
    """
    now = datetime.now(timezone.utc)
    asset_lookup: dict[str, dict] = {}
    if assets_yaml_path and assets_yaml_path.exists():
        asset_lookup = _asset_lookup(_load_assets_yaml(assets_yaml_path))

    crypto_syms = [s for s in symbols if _is_crypto(s)]
    commodity_syms = [s for s in symbols if _is_commodity(s)]

    raw: dict[str, float] = {}
    raw.update(_fetch_crypto_live(crypto_syms))
    raw.update(_fetch_commodity_live(commodity_syms, asset_lookup))

    result: dict[str, PriceResolution] = {}
    for sym, price in raw.items():
        resolution = "minute" if _is_crypto(sym) else "daily"
        result[sym] = PriceResolution(
            asset=sym,
            price=price,
            source="live",
            resolution=resolution,
            fetched_at=now,
        )

    # Cache
    cache_key = f"live_{now.strftime('%Y-%m-%d')}"
    cache_data = {sym: pr.price for sym, pr in result.items()}
    _write_cache(cache_dir, cache_key, cache_data)

    return result


# ---------------------------------------------------------------------------
# Historical price fetchers
# ---------------------------------------------------------------------------


def _fetch_crypto_historical(
    symbols: list[str], as_of: datetime
) -> dict[str, float]:
    """Fetch Binance kline at or before ``as_of`` (1-min resolution)."""
    prices: dict[str, float] = {}
    end_ms = int(as_of.timestamp() * 1000)
    start_ms = end_ms - 60_000  # 1 minute window

    for sym in symbols:
        if not _is_crypto(sym):
            continue
        bsym = _symbol_to_binance(sym)
        if bsym is None:
            continue
        try:
            klines = binance_klines(
                bsym, interval="1m", start_ms=start_ms, end_ms=end_ms, limit=1
            )
            if klines:
                prices[sym] = klines[-1]["close"]
        except Exception as exc:
            logger.warning(
                "Binance historical fetch failed for %s at %s: %s",
                sym,
                as_of.isoformat(),
                exc,
            )
    return prices


def _fetch_commodity_historical(
    symbols: list[str],
    as_of: datetime,
    asset_lookup: dict[str, dict],
) -> dict[str, float]:
    """Fetch yfinance daily close for the trading day at/before ``as_of``."""
    prices: dict[str, float] = {}
    # yfinance needs date range: we ask for a 5-day window ending at as_of
    end_date = as_of.date() + timedelta(days=1)
    start_date = as_of.date() - timedelta(days=5)

    for sym in symbols:
        if not _is_commodity(sym):
            continue
        ticker_str = _symbol_to_yfinance(sym, asset_lookup)
        if ticker_str is None:
            continue
        try:
            import yfinance as yf

            ticker = yf.Ticker(ticker_str)
            hist = ticker.history(
                start=start_date.isoformat(), end=end_date.isoformat()
            )
            if hist is not None and not hist.empty:
                prices[sym] = float(hist["Close"].iloc[-1])
        except Exception as exc:
            logger.warning(
                "yfinance historical fetch failed for %s at %s: %s",
                sym,
                as_of.isoformat(),
                exc,
            )
    return prices


def fetch_historical_prices(
    symbols: list[str],
    as_of: datetime,
    assets_yaml_path: Path | None = None,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> dict[str, PriceResolution]:
    """Fetch prices at/before ``as_of``. Crypto: 1-min Binance; Commodities: daily yfinance.

    Cached under ``data/market/price_cache/{YYYY-MM-DD}.json``. On cache hit,
    no network calls are made.
    """
    now = datetime.now(timezone.utc)
    cache_key = as_of.strftime("%Y-%m-%d")

    # Try cache first
    cached = _read_cache(cache_dir, cache_key)
    if cached is not None:
        result: dict[str, PriceResolution] = {}
        for sym in symbols:
            if sym in cached:
                resolution = "minute" if _is_crypto(sym) else "daily"
                result[sym] = PriceResolution(
                    asset=sym,
                    price=float(cached[sym]),
                    source="historical",
                    resolution=resolution,
                    fetched_at=now,
                )
        if result:
            logger.info("Price cache hit for %s: %d symbols", cache_key, len(result))
            return result

    # Fetch from sources
    asset_lookup: dict[str, dict] = {}
    if assets_yaml_path and assets_yaml_path.exists():
        asset_lookup = _asset_lookup(_load_assets_yaml(assets_yaml_path))

    crypto_syms = [s for s in symbols if _is_crypto(s)]
    commodity_syms = [s for s in symbols if _is_commodity(s)]

    raw: dict[str, float] = {}
    raw.update(_fetch_crypto_historical(crypto_syms, as_of))
    raw.update(_fetch_commodity_historical(commodity_syms, as_of, asset_lookup))

    result = {}
    for sym, price in raw.items():
        resolution = "minute" if _is_crypto(sym) else "daily"
        result[sym] = PriceResolution(
            asset=sym,
            price=price,
            source="historical",
            resolution=resolution,
            fetched_at=now,
        )

    # Write cache
    cache_data = {sym: pr.price for sym, pr in result.items()}
    _write_cache(cache_dir, cache_key, cache_data)

    return result


# ---------------------------------------------------------------------------
# Composite resolver
# ---------------------------------------------------------------------------


def resolve_initial_prices(
    symbols: list[str],
    assets_yaml_path: Path,
    initial_prices: dict[str, float] | None = None,
    price_source: Literal["manual", "live", "historical", "default"] = "default",
    as_of_date: datetime | None = None,
    snap_stablecoins_to_peg: bool = True,
    cache_dir: Path = _DEFAULT_CACHE_DIR,
) -> dict[str, PriceResolution]:
    """Resolve initial prices for all symbols using the priority hierarchy.

    Priority (highest first):
    1. ``initial_prices[symbol]`` -- explicit user overrides.
    2. ``price_source="historical"`` + ``as_of_date`` -- Binance/yfinance fetch.
    3. ``price_source="live"`` -- current market prices.
    4. ``assets.yaml`` ``default_price`` -- static fallback.

    Stablecoin snap: unless a stablecoin is explicitly in ``initial_prices``,
    its price is snapped to ``peg_target`` (1.0 USD) regardless of fetched value.
    """
    now = datetime.now(timezone.utc)
    initial_prices = dict(initial_prices or {})

    # Load asset metadata for defaults + stablecoin detection
    assets = _load_assets_yaml(assets_yaml_path)
    asset_lookup = _asset_lookup(assets)

    result: dict[str, PriceResolution] = {}

    # Step 1: Apply explicit overrides
    explicitly_set: set[str] = set()
    for sym, price in initial_prices.items():
        if sym in symbols:
            result[sym] = PriceResolution(
                asset=sym,
                price=price,
                source="manual",
                resolution="minute",
                fetched_at=now,
            )
            explicitly_set.add(sym)

    # Step 2: Fetch remaining symbols based on price_source
    remaining = [s for s in symbols if s not in explicitly_set and s != "USD"]

    if remaining and price_source == "historical" and as_of_date is not None:
        fetched = fetch_historical_prices(
            remaining, as_of_date, assets_yaml_path, cache_dir
        )
        for sym, pr in fetched.items():
            if sym not in result:
                result[sym] = pr

    elif remaining and price_source == "live":
        fetched = fetch_live_prices(remaining, assets_yaml_path, cache_dir)
        for sym, pr in fetched.items():
            if sym not in result:
                result[sym] = pr

    # Step 3: Fill remaining with defaults from assets.yaml
    for sym in symbols:
        if sym in result:
            continue
        asset_info = asset_lookup.get(sym, {})
        default_price = asset_info.get("default_price")
        if default_price is not None:
            result[sym] = PriceResolution(
                asset=sym,
                price=float(default_price),
                source="default",
                resolution="daily",
                fetched_at=now,
            )
        else:
            # Last resort: 0.0 with a warning
            logger.warning("No price resolved for %s, using 0.0", sym)
            result[sym] = PriceResolution(
                asset=sym,
                price=0.0,
                source="default",
                resolution="daily",
                fetched_at=now,
            )

    # Step 4: Stablecoin snap
    if snap_stablecoins_to_peg:
        for sym in symbols:
            if sym in explicitly_set:
                continue  # user override wins
            asset_info = asset_lookup.get(sym, {})
            peg = asset_info.get("peg_target")
            if peg is not None and asset_info.get("asset_class") == "stablecoin":
                result[sym] = PriceResolution(
                    asset=sym,
                    price=float(peg),
                    source="peg_snap",
                    resolution="minute",
                    fetched_at=now,
                )

    return result
