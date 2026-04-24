"use client";

import { useEffect, useState } from "react";
import { useRunStore } from "@/lib/runStore";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

interface Level {
  price: number;
  size: number;
  count: number;
}

interface OrderbookSnapshot {
  pair: string;
  last_price: number | null;
  bids: Level[];
  asks: Level[];
  spread: number | null;
}

/**
 * L2 depth table for the selected asset pair.
 * 10 levels each side. Bid=green, ask=red, spread+last in cyan at mid.
 *
 * Fetches from /api/runs/{runId}/orderbook when a runId prop is provided
 * (replay / live-from-db). Falls back to Zustand store otherwise.
 */
export function OrderBook({
  runId,
  pair = "BTC/USD",
}: {
  runId?: string;
  pair?: string;
}) {
  const storeBook = useRunStore((s) => s.orderBook);
  const selectedAsset = useRunStore((s) => s.selectedAsset);
  const [snapshot, setSnapshot] = useState<OrderbookSnapshot | null>(null);

  useEffect(() => {
    if (!runId) return;
    let cancelled = false;

    async function poll() {
      try {
        const res = await fetch(
          `${API_BASE}/api/runs/${runId}/orderbook?pair=${encodeURIComponent(pair)}`,
        );
        if (res.status === 404) return;
        if (!res.ok) return;
        const data = (await res.json()) as OrderbookSnapshot;
        if (!cancelled) setSnapshot(data);
      } catch {
        // Swallow network/parse errors; next poll will retry.
      }
    }

    poll();
    const interval = setInterval(poll, 3000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [runId, pair]);

  // Prefer backend snapshot; fall back to store. Normalize both sources to
  // a common Level shape so TypeScript is happy with the union.
  type NormalLevel = { price: number; size: number; count?: number };
  const rawBids: NormalLevel[] = snapshot?.bids ?? storeBook.bids;
  const rawAsks: NormalLevel[] = snapshot?.asks ?? storeBook.asks;
  const last = snapshot?.last_price ?? storeBook.midPrice;
  const spread = snapshot?.spread ?? storeBook.spread;
  const label = snapshot?.pair ?? selectedAsset;

  // Running totals for depth display (cumulative from top of book).
  const withTotal = (levels: NormalLevel[]) => {
    let acc = 0;
    return levels.map((lvl) => {
      acc += lvl.size;
      return { ...lvl, total: acc };
    });
  };
  const topAsks = withTotal(rawAsks.slice(0, 10)).reverse();
  const topBids = withTotal(rawBids.slice(0, 10));

  return (
    <div className="panel">
      <h3 className="panel-title">Order Book ({label})</h3>

      {topAsks.length === 0 && topBids.length === 0 ? (
        <p className="text-dim text-[10px]">Awaiting order book data...</p>
      ) : (
        <div className="text-[10px]">
          <div className="flex text-dim text-[9px] uppercase tracking-widest mb-1">
            <span className="flex-1">Price</span>
            <span className="w-16 text-right">Size</span>
            <span className="w-16 text-right">Total</span>
          </div>

          {topAsks.map((level, i) => (
            <div key={`ask-${i}`} className="flex items-center py-0.5">
              <span className="flex-1 text-bearish tabular-nums">
                {level.price.toFixed(2)}
              </span>
              <span className="w-16 text-right tabular-nums text-dim">
                {level.size.toFixed(4)}
              </span>
              <span className="w-16 text-right tabular-nums text-dim">
                {level.total.toFixed(4)}
              </span>
            </div>
          ))}

          <div className="flex items-center py-1 border-y border-border my-0.5">
            <span className="flex-1 text-cyan font-bold tabular-nums">
              {last && last > 0 ? last.toFixed(2) : "--"}
            </span>
            <span className="text-dim text-[9px]">
              spread: {spread && spread > 0 ? spread.toFixed(2) : "--"}
            </span>
          </div>

          {topBids.map((level, i) => (
            <div key={`bid-${i}`} className="flex items-center py-0.5">
              <span className="flex-1 text-bullish tabular-nums">
                {level.price.toFixed(2)}
              </span>
              <span className="w-16 text-right tabular-nums text-dim">
                {level.size.toFixed(4)}
              </span>
              <span className="w-16 text-right tabular-nums text-dim">
                {level.total.toFixed(4)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
