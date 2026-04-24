"use client";

import { useMemo } from "react";
import { useRunStore } from "@/lib/runStore";

const CHART_HEIGHT = 120;
const PAD = { top: 8, right: 50, bottom: 20, left: 8 };

/** Simple SVG line chart: aggregate wealth over steps. Height 120px. */
export function PnlHistory() {
  const pnl = useRunStore((s) => s.pnl);

  const { path, minW, maxW, width } = useMemo(() => {
    const w = 500;
    const innerW = w - PAD.left - PAD.right;
    const innerH = CHART_HEIGHT - PAD.top - PAD.bottom;

    if (pnl.length === 0) {
      return { path: "", minW: 0, maxW: 0, width: w };
    }

    const vals = pnl.map((p) => p.wealth);
    const mn = Math.min(...vals);
    const mx = Math.max(...vals);
    const range = mx - mn || 1;

    const d = pnl
      .map((pt, i) => {
        const x = PAD.left + (i / Math.max(pnl.length - 1, 1)) * innerW;
        const y = PAD.top + innerH - ((pt.wealth - mn) / range) * innerH;
        return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");

    return { path: d, minW: mn, maxW: mx, width: w };
  }, [pnl]);

  const lastVal = pnl.length > 0 ? pnl[pnl.length - 1].wealth : null;
  const firstVal = pnl.length > 0 ? pnl[0].wealth : null;
  const isUp = lastVal !== null && firstVal !== null && lastVal >= firstVal;

  return (
    <div className="panel">
      <h3 className="panel-title">
        PnL History
        {lastVal !== null && (
          <span
            className={`ml-2 tabular-nums ${isUp ? "text-bullish" : "text-bearish"}`}
          >
            {lastVal.toFixed(2)}
          </span>
        )}
      </h3>
      <svg
        viewBox={`0 0 ${width} ${CHART_HEIGHT}`}
        className="w-full"
        style={{ height: CHART_HEIGHT }}
        aria-label="PnL history chart"
      >
        {pnl.length > 0 ? (
          <>
            {/* Y-axis labels */}
            <text
              x={width - PAD.right + 4}
              y={PAD.top + 3}
              className="fill-dim text-[8px]"
              style={{ fontVariantNumeric: "tabular-nums" }}
            >
              {maxW.toFixed(0)}
            </text>
            <text
              x={width - PAD.right + 4}
              y={CHART_HEIGHT - PAD.bottom + 3}
              className="fill-dim text-[8px]"
              style={{ fontVariantNumeric: "tabular-nums" }}
            >
              {minW.toFixed(0)}
            </text>

            {/* PnL line */}
            <path
              d={path}
              fill="none"
              className={isUp ? "stroke-bullish" : "stroke-bearish"}
              strokeWidth={1.5}
            />
          </>
        ) : (
          <text
            x={width / 2}
            y={CHART_HEIGHT / 2}
            textAnchor="middle"
            className="fill-dim text-[10px]"
          >
            Awaiting PnL data...
          </text>
        )}
      </svg>
    </div>
  );
}
