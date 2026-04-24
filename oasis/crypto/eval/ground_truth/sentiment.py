"""Fear & Greed Index fetcher from alternative.me."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

logger = logging.getLogger(__name__)

_FNG_URL = "https://api.alternative.me/fng/"


def fetch_fear_greed(
    start: str, end: str,
) -> pd.DataFrame:
    """Fetch daily Fear & Greed Index between *start* and *end*.

    Returns DataFrame with columns: date, value, classification.
    """
    try:
        import requests
    except ImportError:
        logger.warning("requests not installed")
        return pd.DataFrame()

    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    days = max(1, (end_dt - start_dt).days + 1)

    try:
        resp = requests.get(
            _FNG_URL, params={"limit": days, "format": "json"}, timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("FNG fetch failed: %s", e)
        return pd.DataFrame()

    items = data.get("data", [])
    if not items:
        return pd.DataFrame()

    rows = []
    for item in items:
        ts = int(item.get("timestamp", 0))
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        if start_dt.date() <= dt.date() <= end_dt.date():
            rows.append({
                "date": dt.date().isoformat(),
                "value": int(item.get("value", 0)),
                "classification": item.get("value_classification", ""),
            })
    return pd.DataFrame(rows)


def parse_fng_response(data: dict) -> pd.DataFrame:
    """Parse a raw FNG API JSON response into a DataFrame.

    Useful for tests with monkey-patched responses.
    """
    items = data.get("data", [])
    rows = []
    for item in items:
        ts = int(item.get("timestamp", 0))
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        rows.append({
            "date": dt.date().isoformat(),
            "value": int(item.get("value", 0)),
            "classification": item.get("value_classification", ""),
        })
    return pd.DataFrame(rows)
