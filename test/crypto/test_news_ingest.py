"""Tests for oasis.crypto.news_ingest (Phase 9a).

All tests are offline -- CryptoPanic is monkey-patched, no Anthropic calls.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oasis.crypto.news_ingest import (
    Audience,
    CryptoPanicAdapter,
    ManualAdapter,
    NewsEvent,
    NewsItem,
    cache_path,
    dedupe_items,
    enrich,
    load_cache,
    mock_enricher,
    save_cache,
    sonnet_enricher_stub,
    wall_clock_to_step,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_MAY_07 = datetime(2022, 5, 7, tzinfo=timezone.utc)
_MAY_08 = datetime(2022, 5, 8, tzinfo=timezone.utc)
_MAY_09 = datetime(2022, 5, 9, tzinfo=timezone.utc)
_MAY_10 = datetime(2022, 5, 10, tzinfo=timezone.utc)
_MAY_11 = datetime(2022, 5, 11, tzinfo=timezone.utc)

MANUAL_ITEMS: list[dict] = [
    {
        "timestamp": "2022-05-07T10:00:00+00:00",
        "title": "BTC drops below $35k amid Terra panic",
        "body": "Bitcoin has fallen sharply as the USDT peg weakens.",
        "source_prior_sentiment": "bearish",
        "source_id": "m1",
    },
    {
        "timestamp": "2022-05-08T14:30:00+00:00",
        "title": "ETH network congestion spikes",
        "body": "Ethereum gas fees soar due to liquidation cascades.",
        "source_prior_sentiment": "neutral",
        "source_id": "m2",
    },
    {
        "timestamp": "2022-05-10T08:00:00+00:00",
        "title": "Gold rallies as risk-off sentiment grows",
        "body": "Investors flock to XAU as crypto bleeds.",
        "source_prior_sentiment": "bullish",
        "source_id": "m3",
    },
    {
        "timestamp": "2022-05-12T00:00:00+00:00",
        "title": "Outside the window",
        "source_id": "m4",
    },
]

# A CryptoPanic-style response fixture (2 posts)
CRYPTOPANIC_FIXTURE: dict = {
    "count": 2,
    "next": None,
    "results": [
        {
            "id": 12345,
            "published_at": "2022-05-09T12:00:00Z",
            "title": "BTC whale moves 5000 coins to exchange",
            "body": None,
            "url": "https://example.com/btc-whale",
            "currencies": [{"code": "BTC", "title": "Bitcoin"}],
            "votes": {"positive": 3, "negative": 10, "important": 1},
            "domain": "example.com",
        },
        {
            "id": 12346,
            "published_at": "2022-05-09T18:30:00Z",
            "title": "Ethereum merge update: testnet launch date confirmed",
            "body": "The Ropsten testnet merge is scheduled for June.",
            "url": "https://example.com/eth-merge",
            "currencies": [{"code": "ETH", "title": "Ethereum"}],
            "votes": {"positive": 15, "negative": 2, "important": 5},
            "domain": "example.com",
        },
    ],
}


# ---------------------------------------------------------------------------
# 1. ManualAdapter loads items, filters by date range
# ---------------------------------------------------------------------------

class TestManualAdapter:
    def test_filters_by_date_range(self):
        adapter = ManualAdapter(items=MANUAL_ITEMS)
        items = adapter.fetch(_MAY_07, _MAY_09)
        # Should include items on May 7 and May 8, not May 10 or 12
        assert len(items) == 2
        assert items[0].source_id == "m1"
        assert items[1].source_id == "m2"

    def test_full_range(self):
        adapter = ManualAdapter(items=MANUAL_ITEMS)
        items = adapter.fetch(_MAY_07, _MAY_11)
        assert len(items) == 3  # excludes May 12
        assert all(i.source == "manual" for i in items)

    def test_empty_range(self):
        adapter = ManualAdapter(items=MANUAL_ITEMS)
        items = adapter.fetch(
            datetime(2020, 1, 1, tzinfo=timezone.utc),
            datetime(2020, 1, 2, tzinfo=timezone.utc),
        )
        assert items == []

    def test_fields_populated(self):
        adapter = ManualAdapter(items=MANUAL_ITEMS)
        items = adapter.fetch(_MAY_07, _MAY_08)
        item = items[0]
        assert item.title == "BTC drops below $35k amid Terra panic"
        assert item.body == "Bitcoin has fallen sharply as the USDT peg weakens."
        assert item.source_prior_sentiment == "bearish"
        assert item.source == "manual"


# ---------------------------------------------------------------------------
# 2. CryptoPanicAdapter with monkey-patched requests.get
# ---------------------------------------------------------------------------

class TestCryptoPanicAdapter:
    def test_fetch_with_fixture(self, monkeypatch):
        """Verify parsed NewsItems have correct fields from fixture data."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = CRYPTOPANIC_FIXTURE
        mock_response.raise_for_status = MagicMock()

        import requests
        monkeypatch.setattr(requests, "get", MagicMock(return_value=mock_response))

        adapter = CryptoPanicAdapter(auth_token=None)
        items = adapter.fetch(_MAY_09, _MAY_10)

        assert len(items) == 2

        # First item (by appearance in results — reverse chrono, so id=12345 first)
        whale = items[0]
        assert whale.source == "cryptopanic"
        assert whale.source_id == "12345"
        assert whale.title == "BTC whale moves 5000 coins to exchange"
        assert whale.url == "https://example.com/btc-whale"
        assert whale.source_prior_sentiment == "bearish"  # negative > positive
        assert whale.raw_metadata["currencies"] == ["BTC"]

        # Second item
        eth = items[1]
        assert eth.source_id == "12346"
        assert eth.title == "Ethereum merge update: testnet launch date confirmed"
        assert eth.source_prior_sentiment == "bullish"  # positive > negative > important
        assert eth.body == "The Ropsten testnet merge is scheduled for June."

    def test_request_params(self, monkeypatch):
        """Verify the shape of the outgoing request."""
        captured_args: dict = {}

        def fake_get(url, params=None, timeout=None):
            captured_args["url"] = url
            captured_args["params"] = params
            captured_args["timeout"] = timeout
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"results": [], "next": None}
            mock_resp.raise_for_status = MagicMock()
            return mock_resp

        import requests
        monkeypatch.setattr(requests, "get", fake_get)

        adapter = CryptoPanicAdapter(auth_token="test_token", timeout=5.0)
        adapter.fetch(_MAY_09, _MAY_10)

        assert captured_args["url"] == "https://cryptopanic.com/api/v1/posts/"
        assert captured_args["params"]["public"] == "true"
        assert captured_args["params"]["kind"] == "news"
        assert captured_args["params"]["auth_token"] == "test_token"
        assert captured_args["timeout"] == 5.0


# ---------------------------------------------------------------------------
# 3 & 4. mock_enricher
# ---------------------------------------------------------------------------

class TestMockEnricher:
    def test_bearish_btc_item(self):
        """BTC-mentioning bearish item produces expected enrichment."""
        item = NewsItem(
            source="manual",
            source_id="t1",
            timestamp=_MAY_09,
            title="BTC drops below $35k amid Terra panic",
            body="Bitcoin crash continues.",
            source_prior_sentiment="bearish",
        )
        events = mock_enricher([item])
        assert len(events) == 1
        ev = events[0]
        assert ev.sentiment_valence == -0.5
        assert ev.affected_assets == ["BTC"]
        assert ev.magnitude == "moderate"
        assert ev.credibility == "reported"
        assert ev.enricher == "mock"
        assert ev.audience == Audience.ALL

    def test_bullish_multi_asset(self):
        item = NewsItem(
            source="manual",
            timestamp=_MAY_09,
            title="ETH and Gold rally together",
            body="Ethereum surges while XAU hits new highs.",
            source_prior_sentiment="bullish",
        )
        events = mock_enricher([item])
        ev = events[0]
        assert ev.sentiment_valence == 0.5
        assert "ETH" in ev.affected_assets
        assert "XAU" in ev.affected_assets

    def test_no_sentiment_defaults_zero(self):
        item = NewsItem(
            source="manual",
            timestamp=_MAY_09,
            title="Market update",
        )
        events = mock_enricher([item])
        assert events[0].sentiment_valence == 0.0

    def test_deterministic(self):
        """Same input produces identical output on two calls."""
        items = [
            NewsItem(
                source="manual",
                source_id="det1",
                timestamp=_MAY_09,
                title="BTC surges past $40k",
                source_prior_sentiment="bullish",
            ),
            NewsItem(
                source="manual",
                source_id="det2",
                timestamp=_MAY_09,
                title="Oil drops on demand fears",
                body="WTI crude falls 3%",
                source_prior_sentiment="bearish",
            ),
        ]
        result_a = mock_enricher(items)
        result_b = mock_enricher(items)
        for a, b in zip(result_a, result_b):
            assert a.model_dump() == b.model_dump()


# ---------------------------------------------------------------------------
# 5. sonnet_enricher_stub raises NotImplementedError
# ---------------------------------------------------------------------------

def test_sonnet_enricher_stub_raises():
    with pytest.raises(NotImplementedError, match="wire Claude Sonnet"):
        sonnet_enricher_stub([
            NewsItem(source="test", timestamp=_MAY_09, title="whatever"),
        ])


# ---------------------------------------------------------------------------
# 6. enrich([]) returns []
# ---------------------------------------------------------------------------

def test_enrich_empty():
    assert enrich([]) == []


# ---------------------------------------------------------------------------
# 7. save_cache + load_cache round-trip
# ---------------------------------------------------------------------------

def test_cache_round_trip(tmp_path):
    events = [
        NewsEvent(
            source="manual",
            source_id="rt1",
            timestamp=datetime(2022, 5, 9, 6, 12, tzinfo=timezone.utc),
            title="Round-trip test event",
            body="Body content with unicode: — ₿",
            url="https://example.com/test",
            sentiment_valence=-0.3,
            affected_assets=["BTC", "ETH"],
            audience=Audience.NEWS_TRADERS,
            magnitude="major",
            credibility="confirmed",
            enricher="mock",
        ),
        NewsEvent(
            source="cryptopanic",
            source_id=None,
            timestamp=datetime(2022, 5, 10, 0, 0, tzinfo=timezone.utc),
            title="Null source_id event",
            body=None,
            url=None,
            sentiment_valence=0.0,
            affected_assets=[],
            audience=Audience.ALL,
            magnitude="minor",
            credibility="rumor",
            enricher="mock",
        ),
    ]
    path = tmp_path / "test_cache.jsonl"
    save_cache(events, path)
    loaded = load_cache(path)

    assert len(loaded) == 2
    for orig, restored in zip(events, loaded):
        assert orig.model_dump() == restored.model_dump()
        # Verify timezone awareness is preserved
        assert restored.timestamp.tzinfo is not None


def test_load_cache_missing_file(tmp_path):
    assert load_cache(tmp_path / "nonexistent.jsonl") == []


# ---------------------------------------------------------------------------
# 8. wall_clock_to_step correct value
# ---------------------------------------------------------------------------

def test_wall_clock_to_step_luna_example():
    scenario_start = datetime(2022, 5, 7, 0, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2022, 5, 9, 6, 12, 0, tzinfo=timezone.utc)
    step = wall_clock_to_step(ts, scenario_start, step_minutes=1)
    # (2 days * 24h * 60m) + (6h * 60m) + 12m = 2880 + 360 + 12 = 3252
    assert step == 3252


# ---------------------------------------------------------------------------
# 9. wall_clock_to_step raises ValueError for ts < scenario_start
# ---------------------------------------------------------------------------

def test_wall_clock_to_step_before_start_raises():
    scenario_start = datetime(2022, 5, 9, 0, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2022, 5, 7, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="precedes scenario start"):
        wall_clock_to_step(ts, scenario_start)


def test_wall_clock_to_step_exact_start():
    start = datetime(2022, 5, 7, 0, 0, 0, tzinfo=timezone.utc)
    assert wall_clock_to_step(start, start) == 0


def test_wall_clock_to_step_custom_interval():
    start = datetime(2022, 5, 7, 0, 0, 0, tzinfo=timezone.utc)
    ts = datetime(2022, 5, 7, 1, 0, 0, tzinfo=timezone.utc)
    assert wall_clock_to_step(ts, start, step_minutes=5) == 12


# ---------------------------------------------------------------------------
# 10. cache_path returns expected format
# ---------------------------------------------------------------------------

def test_cache_path_format():
    base = Path("/data/market")
    start = datetime(2022, 5, 7, tzinfo=timezone.utc)
    end = datetime(2022, 5, 11, tzinfo=timezone.utc)
    p = cache_path(base, start, end)
    assert p == Path("/data/market/news_cache/2022-05-07_to_2022-05-11.jsonl")


# ---------------------------------------------------------------------------
# 11. CLI smoke test: --help exits 0
# ---------------------------------------------------------------------------

def test_cli_help():
    script = str(Path(__file__).resolve().parents[2] / "scripts" / "ingest_news.py")
    result = subprocess.run(
        [sys.executable, script, "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "enricher" in result.stdout.lower()
    assert "providers" in result.stdout.lower()


# ---------------------------------------------------------------------------
# 12. CLI with manual provider + mock enricher round-trips through cache
# ---------------------------------------------------------------------------

def test_cli_manual_round_trip(tmp_path):
    import yaml

    # Write a manual events YAML file
    events_data = [
        {
            "timestamp": "2022-05-08T10:00:00+00:00",
            "title": "BTC flash crash",
            "body": "Bitcoin dropped 10% in minutes",
            "source_prior_sentiment": "bearish",
            "source_id": "cli_test_1",
        },
        {
            "timestamp": "2022-05-08T15:00:00+00:00",
            "title": "ETH gas spikes to 500 gwei",
            "source_prior_sentiment": "neutral",
            "source_id": "cli_test_2",
        },
    ]
    yaml_file = tmp_path / "test_events.yaml"
    with open(yaml_file, "w") as f:
        yaml.dump(events_data, f)

    output_dir = tmp_path / "output"
    script = str(Path(__file__).resolve().parents[2] / "scripts" / "ingest_news.py")

    result = subprocess.run(
        [
            sys.executable, script,
            "--providers", "manual",
            "--manual-file", str(yaml_file),
            "--start", "2022-05-07",
            "--end", "2022-05-09",
            "--enricher", "mock",
            "--output", str(output_dir),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"CLI failed: {result.stderr}"
    assert "2 items fetched" in result.stdout
    assert "2 enriched" in result.stdout

    # Verify the cache file was written and can be loaded
    expected_cache = output_dir / "news_cache" / "2022-05-07_to_2022-05-09.jsonl"
    assert expected_cache.exists()
    loaded = load_cache(expected_cache)
    assert len(loaded) == 2
    assert loaded[0].title == "BTC flash crash"
    assert loaded[0].sentiment_valence == -0.5
    assert loaded[0].affected_assets == ["BTC"]


# ---------------------------------------------------------------------------
# Extra: dedupe helper
# ---------------------------------------------------------------------------

def test_dedupe_items():
    items = [
        NewsItem(source="a", source_id="1", timestamp=_MAY_09, title="First"),
        NewsItem(source="a", source_id="1", timestamp=_MAY_09, title="Dupe"),
        NewsItem(source="b", source_id="1", timestamp=_MAY_09, title="Diff source"),
        NewsItem(source="a", source_id=None, timestamp=_MAY_09, title="No ID 1"),
        NewsItem(source="a", source_id=None, timestamp=_MAY_09, title="No ID 2"),
    ]
    deduped = dedupe_items(items)
    assert len(deduped) == 4
    assert deduped[0].title == "First"
    assert deduped[1].title == "Diff source"
