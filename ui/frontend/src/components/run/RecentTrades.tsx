"use client";

import { useRunStore } from "@/lib/runStore";

/** Tape-style trade list: step | side chip | price | qty | user_name. Last 20. */
export function RecentTrades() {
  const trades = useRunStore((s) => s.trades);
  const recent = trades.slice(-20);

  return (
    <div className="panel">
      <h3 className="panel-title">Recent Trades</h3>
      {recent.length === 0 ? (
        <p className="text-dim text-[10px]">No trades yet...</p>
      ) : (
        <div className="text-[10px]">
          {/* Header */}
          <div className="flex text-dim text-[9px] uppercase tracking-widest mb-1">
            <span className="w-8">Step</span>
            <span className="w-10">Side</span>
            <span className="flex-1 text-right">Price</span>
            <span className="w-16 text-right">Qty</span>
            <span className="w-20 text-right">Agent</span>
          </div>

          {recent.map((t, i) => (
            <div
              key={`${t.step}-${t.user_name}-${i}`}
              className="flex items-center py-0.5 border-b border-border/30 last:border-0"
            >
              <span className="w-8 text-dim tabular-nums">s{t.step}</span>
              <span className="w-10">
                <span
                  className={`px-1 py-0.5 text-[9px] font-bold ${
                    t.side === "buy"
                      ? "text-bullish bg-bullish/10"
                      : "text-bearish bg-bearish/10"
                  }`}
                >
                  {t.side.toUpperCase()}
                </span>
              </span>
              <span className="flex-1 text-right tabular-nums text-text">
                {t.price.toFixed(2)}
              </span>
              <span className="w-16 text-right tabular-nums text-dim">
                {t.qty.toFixed(4)}
              </span>
              <span className="w-20 text-right text-purple truncate" title={t.user_name}>
                {t.user_name}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
