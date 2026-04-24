"use client";

import { useEffect, useMemo, useState } from "react";
import { useRunStore } from "@/lib/runStore";

function utcClock(): string {
  const d = new Date();
  return d.toISOString().replace("T", " ").slice(0, 19) + " UTC";
}

const TICKER_ORDER = ["BTC", "ETH", "USDT", "XAU", "WTI"];

export function TopBar() {
  const [time, setTime] = useState<string>("");
  const prices = useRunStore((s) => s.prices);

  useEffect(() => {
    setTime(utcClock());
    const id = setInterval(() => setTime(utcClock()), 1000);
    return () => clearInterval(id);
  }, []);

  // Build per-asset ticker from the store: {asset: [PricePoint...]}.
  // For each asset compute latest price + % change from first recorded
  // price. Active only on run pages that populate the store.
  const tickers = useMemo(() => {
    const out: { sym: string; last: number; pctChange: number }[] = [];
    for (const sym of TICKER_ORDER) {
      const series = prices[sym];
      if (!series || series.length === 0) continue;
      const first = series[0]?.price ?? 0;
      const last = series[series.length - 1]?.price ?? 0;
      const pct = first > 0 ? ((last - first) / first) * 100 : 0;
      out.push({ sym, last, pctChange: pct });
    }
    return out;
  }, [prices]);

  return (
    <header className="flex items-center justify-between px-4 py-2 border-b border-border-bright bg-panel">
      {/* Left: branding — DeSimulator wordmark with Defily purple glow */}
      <div className="flex items-center gap-3">
        <a
          href="https://www.defily.ai/"
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-2 group"
        >
          <span
            aria-hidden="true"
            className="inline-block w-2 h-2 rounded-full bg-cyan"
            style={{
              boxShadow:
                "0 0 10px rgba(122,47,244,0.9), 0 0 24px rgba(122,47,244,0.45)",
            }}
          />
          <span className="brand-glow text-cyan font-bold text-sm tracking-[0.18em]">
            DeSimulator
          </span>
          <span className="text-dim text-[9px] uppercase tracking-widest transition-colors group-hover:text-text">
            by Defily
          </span>
        </a>
        <span className="hidden lg:inline text-dim text-[10px] uppercase tracking-widest border-l border-border pl-3">
          Narrative Market Sim
        </span>
      </div>

      {/* Center: live price ticker. Shows fallback only when we're not on
          a run page (store has no price series). */}
      <div className="hidden md:flex items-center gap-4 text-[11px]">
        {tickers.length === 0 ? (
          <span className="text-dim">Open a run to see live prices</span>
        ) : (
          tickers.map((t) => {
            const color =
              t.pctChange > 0
                ? "text-bullish"
                : t.pctChange < 0
                  ? "text-bearish"
                  : "text-dim";
            return (
              <span key={t.sym} className="flex items-baseline gap-1 tabular-nums">
                <span className="text-dim text-[10px] uppercase">{t.sym}</span>
                <span className="text-text">{t.last.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
                <span className={`${color} text-[10px]`}>
                  {t.pctChange >= 0 ? "+" : ""}{t.pctChange.toFixed(2)}%
                </span>
              </span>
            );
          })
        )}
      </div>

      {/* Right: UTC clock + status */}
      <div className="flex items-center gap-3 text-[11px]">
        <span className="text-dim font-mono tabular-nums">{time}</span>
        <span className="live-dot text-[10px] text-cyan">READY</span>
      </div>
    </header>
  );
}
