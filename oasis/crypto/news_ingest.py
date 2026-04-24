"""News ingestion framework: adapters, enrichment interface, and cache.

Phase 9a deliverable. Provides:
- NewsItem / NewsEvent dataclasses
- NewsAdapter base + ManualAdapter + CryptoPanicAdapter
- Enrichment interface (mock + sonnet stub)
- JSONL cache layer
- wall_clock_to_step utility
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable, Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Audience(str, Enum):
    ALL = "all"
    NEWS_TRADERS = "news_traders"
    KOLS = "kols"
    CRYPTO_NATIVES = "crypto_natives"
    WHALES = "whales"


# ---------------------------------------------------------------------------
# Core data models
# ---------------------------------------------------------------------------

class NewsItem(BaseModel):
    """Raw news as fetched from an adapter."""

    source: str                         # "cryptopanic", "manual", etc.
    source_id: str | None = None        # stable per-source identifier (for dedup)
    timestamp: datetime                 # UTC
    title: str
    body: str | None = None
    url: str | None = None
    source_prior_sentiment: (
        Literal["bullish", "bearish", "neutral", "important"] | None
    ) = None
    raw_metadata: dict = Field(default_factory=dict)


class NewsEvent(BaseModel):
    """Enriched news ready for the simulation."""

    source: str
    source_id: str | None
    timestamp: datetime
    title: str
    body: str | None
    url: str | None

    # Enrichment fields
    sentiment_valence: float = Field(ge=-1.0, le=1.0)
    affected_assets: list[str]          # e.g. ["BTC", "ETH"]
    audience: Audience
    magnitude: Literal["minor", "moderate", "major", "critical"]
    credibility: Literal["rumor", "reported", "confirmed"]
    enricher: str                       # which enricher tagged this


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------

class NewsAdapter:
    """Base class. Subclasses implement fetch(start, end) -> list[NewsItem]."""

    source_name: str = "base"

    def fetch(self, start: datetime, end: datetime) -> list[NewsItem]:
        raise NotImplementedError


class ManualAdapter(NewsAdapter):
    """Reads from an in-memory list of dicts (or YAML-loaded structure).

    Each dict should have at minimum ``timestamp`` and ``title``.  Optional
    keys: ``body``, ``url``, ``source_id``, ``source_prior_sentiment``,
    plus any extra keys stored in ``raw_metadata``.

    ``timestamp`` may be a datetime object or an ISO-8601 string.
    ``fetch()`` filters by the requested date range.
    """

    source_name = "manual"

    _KNOWN_KEYS = {
        "timestamp", "title", "body", "url", "source_id",
        "source_prior_sentiment",
    }

    def __init__(self, items: list[dict]) -> None:
        self._raw_items = items

    def fetch(self, start: datetime, end: datetime) -> list[NewsItem]:
        start = _ensure_utc(start)
        end = _ensure_utc(end)
        result: list[NewsItem] = []
        for raw in self._raw_items:
            ts = _parse_timestamp(raw["timestamp"])
            if ts < start or ts > end:
                continue
            extra = {k: v for k, v in raw.items() if k not in self._KNOWN_KEYS}
            result.append(NewsItem(
                source="manual",
                source_id=raw.get("source_id"),
                timestamp=ts,
                title=raw["title"],
                body=raw.get("body"),
                url=raw.get("url"),
                source_prior_sentiment=raw.get("source_prior_sentiment"),
                raw_metadata=extra,
            ))
        return result


class CryptoPanicAdapter(NewsAdapter):
    """Fetches news from the CryptoPanic free-tier API.

    Endpoint: ``https://cryptopanic.com/api/v1/posts/``

    Uses ``public=true`` when no auth_token is provided.  Paginates through
    ``next`` links to collect all results in the requested window.

    **Never called in tests** -- tests monkey-patch ``fetch`` or mock
    ``requests.get``.
    """

    source_name = "cryptopanic"
    BASE_URL = "https://cryptopanic.com/api/v1/posts/"

    def __init__(
        self,
        auth_token: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._auth_token = auth_token
        self._timeout = timeout

    def fetch(self, start: datetime, end: datetime) -> list[NewsItem]:
        import requests

        start = _ensure_utc(start)
        end = _ensure_utc(end)

        params: dict = {
            "public": "true",
            "kind": "news",
        }
        if self._auth_token:
            params["auth_token"] = self._auth_token

        all_items: list[NewsItem] = []
        url: str | None = self.BASE_URL

        while url is not None:
            resp = requests.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()

            for post in data.get("results", []):
                ts = _parse_timestamp(post.get("published_at", ""))
                if ts < start:
                    # Results are reverse-chronological; once we pass start
                    # we can stop paginating.
                    url = None
                    break
                if ts > end:
                    continue
                # Extract currencies mentioned
                currencies = [
                    c.get("code", "")
                    for c in post.get("currencies", [])
                ]
                # Map CryptoPanic votes to prior sentiment
                votes = post.get("votes", {})
                prior = _votes_to_sentiment(votes)

                all_items.append(NewsItem(
                    source="cryptopanic",
                    source_id=str(post.get("id", "")),
                    timestamp=ts,
                    title=post.get("title", ""),
                    body=post.get("body") or None,
                    url=post.get("url") or post.get("source", {}).get("url"),
                    source_prior_sentiment=prior,
                    raw_metadata={
                        "currencies": currencies,
                        "votes": votes,
                        "domain": post.get("domain", ""),
                    },
                ))

            # Follow pagination (only when url wasn't set to None above)
            if url is not None:
                url = data.get("next")
                # After the first request, params are baked into the next URL
                params = {}

        return all_items


# ---------------------------------------------------------------------------
# Enrichment interface
# ---------------------------------------------------------------------------

EnricherFn = Callable[[list[NewsItem]], list[NewsEvent]]

# Keyword patterns for simple asset detection in mock_enricher
_ASSET_PATTERNS: dict[str, re.Pattern] = {
    "BTC": re.compile(r"\b(BTC|Bitcoin)\b", re.IGNORECASE),
    "ETH": re.compile(r"\b(ETH|Ethereum|Ether)\b", re.IGNORECASE),
    "USDT": re.compile(r"\b(USDT|Tether)\b", re.IGNORECASE),
    "XAU": re.compile(r"\b(XAU|Gold)\b", re.IGNORECASE),
    "WTI": re.compile(r"\b(WTI|Oil|Crude)\b", re.IGNORECASE),
}

_SENTIMENT_MAP: dict[str | None, float] = {
    "bullish": 0.5,
    "bearish": -0.5,
    "neutral": 0.0,
    "important": 0.0,
    None: 0.0,
}


def mock_enricher(items: list[NewsItem]) -> list[NewsEvent]:
    """Deterministic stub enricher.

    - Sentiment from ``source_prior_sentiment``: bullish=+0.5, bearish=-0.5,
      neutral/important/None=0.0.
    - Affected assets via simple keyword matching on title+body.
    - audience='all', magnitude='moderate', credibility='reported',
      enricher='mock'.
    """
    events: list[NewsEvent] = []
    for item in items:
        text = item.title or ""
        if item.body:
            text = text + " " + item.body

        # Detect assets
        assets = [
            symbol
            for symbol, pat in _ASSET_PATTERNS.items()
            if pat.search(text)
        ]

        valence = _SENTIMENT_MAP.get(item.source_prior_sentiment, 0.0)

        events.append(NewsEvent(
            source=item.source,
            source_id=item.source_id,
            timestamp=item.timestamp,
            title=item.title,
            body=item.body,
            url=item.url,
            sentiment_valence=valence,
            affected_assets=assets,
            audience=Audience.ALL,
            magnitude="moderate",
            credibility="reported",
            enricher="mock",
        ))
    return events


def sonnet_enricher_stub(items: list[NewsItem]) -> list[NewsEvent]:
    """Placeholder for the Claude Sonnet enricher.

    Will be implemented alongside Phase 4 persona generation (same Anthropic
    API plumbing). Do not actually call Anthropic in this phase.
    """
    raise NotImplementedError("wire Claude Sonnet 4.6 here")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def enrich(
    items: list[NewsItem],
    enricher: EnricherFn = mock_enricher,
) -> list[NewsEvent]:
    """Apply the enricher function to a list of raw news items.

    Returns a list of enriched :class:`NewsEvent` objects.  No batching
    logic here -- that is an enricher implementation detail.
    """
    if not items:
        return []
    return enricher(items)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def cache_path(base_dir: Path, start: datetime, end: datetime) -> Path:
    """Return ``<base_dir>/news_cache/YYYY-MM-DD_to_YYYY-MM-DD.jsonl``."""
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    return base_dir / "news_cache" / f"{start_str}_to_{end_str}.jsonl"


def save_cache(events: list[NewsEvent], path: Path) -> None:
    """Persist enriched events as JSONL (one JSON object per line).

    Creates parent directories if they don't exist.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for ev in events:
            f.write(ev.model_dump_json() + "\n")


def load_cache(path: Path) -> list[NewsEvent]:
    """Load enriched events from a JSONL file.

    Returns an empty list if the file does not exist.
    """
    if not path.exists():
        return []
    events: list[NewsEvent] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(NewsEvent.model_validate_json(line))
    return events


# ---------------------------------------------------------------------------
# Step mapping
# ---------------------------------------------------------------------------

def wall_clock_to_step(
    ts: datetime,
    scenario_start: datetime,
    step_minutes: int = 1,
) -> int:
    """Map a UTC timestamp to a simulation step index.

    ``step = floor((ts - scenario_start).total_seconds() / (step_minutes * 60))``

    Raises :class:`ValueError` if *ts* precedes *scenario_start*.
    """
    ts = _ensure_utc(ts)
    scenario_start = _ensure_utc(scenario_start)

    delta = (ts - scenario_start).total_seconds()
    if delta < 0:
        raise ValueError(
            f"Timestamp {ts.isoformat()} precedes scenario start "
            f"{scenario_start.isoformat()}"
        )
    return int(math.floor(delta / (step_minutes * 60)))


# ---------------------------------------------------------------------------
# Deduplication helper (used by the CLI)
# ---------------------------------------------------------------------------

def dedupe_items(items: list[NewsItem]) -> list[NewsItem]:
    """Remove duplicates by (source, source_id). Keeps first occurrence.

    Items with ``source_id=None`` are never considered duplicates of each
    other (they have no stable identity).
    """
    seen: set[tuple[str, str]] = set()
    result: list[NewsItem] = []
    for item in items:
        if item.source_id is not None:
            key = (item.source, item.source_id)
            if key in seen:
                continue
            seen.add(key)
        result.append(item)
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_utc(dt: datetime) -> datetime:
    """Attach UTC timezone if the datetime is naive."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_timestamp(raw: str | datetime) -> datetime:
    """Parse an ISO-8601 string or pass through a datetime.

    Always returns a timezone-aware datetime (UTC if naive).
    """
    if isinstance(raw, datetime):
        return _ensure_utc(raw)
    # Handle the trailing 'Z' that Python <3.11 can't parse with fromisoformat
    cleaned = raw.replace("Z", "+00:00")
    return _ensure_utc(datetime.fromisoformat(cleaned))


def _votes_to_sentiment(
    votes: dict,
) -> Literal["bullish", "bearish", "neutral", "important"] | None:
    """Derive a prior sentiment from CryptoPanic vote tallies."""
    if not votes:
        return None
    positive = votes.get("positive", 0)
    negative = votes.get("negative", 0)
    important = votes.get("important", 0)
    if important > positive and important > negative:
        return "important"
    if positive > negative:
        return "bullish"
    if negative > positive:
        return "bearish"
    return "neutral"
