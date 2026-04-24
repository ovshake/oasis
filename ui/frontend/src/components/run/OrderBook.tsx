"use client";

import { useRunStore } from "@/lib/runStore";

/**
 * L2 depth table for the selected asset pair.
 * 10 levels each side. Bid=green, ask=red, spread in cyan at mid.
 */
export function OrderBook() {
  const { bids, asks, spread, midPrice } = useRunStore((s) => s.orderBook);
  const selectedAsset = useRunStore((s) => s.selectedAsset);

  const topAsks = asks.slice(0, 10).reverse();
  const topBids = bids.slice(0, 10);

  return (
    <div className="panel">
      <h3 className="panel-title">Order Book ({selectedAsset})</h3>

      {topAsks.length === 0 && topBids.length === 0 ? (
        <p className="text-dim text-[10px]">Awaiting order book data...</p>
      ) : (
        <div className="text-[10px]">
          {/* Header */}
          <div className="flex text-dim text-[9px] uppercase tracking-widest mb-1">
            <span className="flex-1">Price</span>
            <span className="w-16 text-right">Size</span>
            <span className="w-16 text-right">Total</span>
          </div>

          {/* Asks (reversed so lowest ask is closest to spread) */}
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

          {/* Spread */}
          <div className="flex items-center py-1 border-y border-border my-0.5">
            <span className="flex-1 text-cyan font-bold tabular-nums">
              {midPrice > 0 ? midPrice.toFixed(2) : "--"}
            </span>
            <span className="text-dim text-[9px]">
              spread: {spread > 0 ? spread.toFixed(2) : "--"}
            </span>
          </div>

          {/* Bids */}
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
