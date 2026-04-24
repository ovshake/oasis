"use client";

import { useMemo, useState } from "react";
import { useRunStore } from "@/lib/runStore";
import type { PricePoint } from "@/lib/types";

const CHART_HEIGHT = 280;
const CHART_PADDING = { top: 16, right: 60, bottom: 28, left: 8 };

/**
 * SVG line chart for a selected asset's price series.
 * Asset-picker tabs above. Tabular-nums tooltip on hover.
 */
export function PriceChart() {
  const assets = useRunStore((s) => s.assets);
  const selectedAsset = useRunStore((s) => s.selectedAsset);
  const setSelectedAsset = useRunStore((s) => s.setSelectedAsset);
  const allPrices = useRunStore((s) => s.prices);
  const points: PricePoint[] = useMemo(
    () => allPrices[selectedAsset] ?? [],
    [allPrices, selectedAsset],
  );

  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  const { path, minP, maxP, xScale, yScale, width } = useMemo(() => {
    const w = 600; // nominal SVG width
    const innerW = w - CHART_PADDING.left - CHART_PADDING.right;
    const innerH = CHART_HEIGHT - CHART_PADDING.top - CHART_PADDING.bottom;

    if (points.length === 0) {
      return { path: "", minP: 0, maxP: 0, xScale: () => 0, yScale: () => 0, width: w };
    }

    const prices = points.map((p) => p.price);
    const mn = Math.min(...prices);
    const mx = Math.max(...prices);
    const range = mx - mn || 1;

    const xs = (i: number) =>
      CHART_PADDING.left + (i / Math.max(points.length - 1, 1)) * innerW;
    const ys = (p: number) =>
      CHART_PADDING.top + innerH - ((p - mn) / range) * innerH;

    const d = points
      .map((pt, i) => `${i === 0 ? "M" : "L"}${xs(i).toFixed(1)},${ys(pt.price).toFixed(1)}`)
      .join(" ");

    return { path: d, minP: mn, maxP: mx, xScale: xs, yScale: ys, width: w };
  }, [points]);

  const hoverPoint = hoverIdx !== null ? points[hoverIdx] : null;

  return (
    <div className="panel">
      <div className="flex items-center justify-between mb-2">
        <h3 className="panel-title mb-0">Price Chart</h3>
        {/* Asset tabs */}
        <div className="flex gap-1">
          {(assets.length > 0 ? assets : ["BTC"]).map((a) => (
            <button
              key={a}
              onClick={() => setSelectedAsset(a)}
              className={`px-2 py-0.5 text-[10px] font-bold border transition-colors ${
                a === selectedAsset
                  ? "border-cyan/50 text-cyan bg-cyan/10"
                  : "border-border text-dim hover:border-border-bright"
              }`}
              aria-label={`Show ${a} price chart`}
            >
              {a}
            </button>
          ))}
        </div>
      </div>

      <svg
        viewBox={`0 0 ${width} ${CHART_HEIGHT}`}
        className="w-full"
        style={{ height: CHART_HEIGHT }}
        aria-label={`Price chart for ${selectedAsset}`}
        onMouseMove={(e) => {
          if (points.length === 0) return;
          const rect = e.currentTarget.getBoundingClientRect();
          const x = ((e.clientX - rect.left) / rect.width) * width;
          const idx = Math.round(
            ((x - CHART_PADDING.left) /
              (width - CHART_PADDING.left - CHART_PADDING.right)) *
              (points.length - 1),
          );
          setHoverIdx(Math.max(0, Math.min(idx, points.length - 1)));
        }}
        onMouseLeave={() => setHoverIdx(null)}
      >
        {/* Grid lines */}
        {points.length > 0 &&
          [0, 0.25, 0.5, 0.75, 1].map((f) => {
            const price = minP + (maxP - minP) * f;
            const y = yScale(price);
            return (
              <g key={f}>
                <line
                  x1={CHART_PADDING.left}
                  x2={width - CHART_PADDING.right}
                  y1={y}
                  y2={y}
                  className="stroke-border"
                  strokeWidth={0.5}
                />
                <text
                  x={width - CHART_PADDING.right + 4}
                  y={y + 3}
                  className="fill-dim text-[9px]"
                  style={{ fontVariantNumeric: "tabular-nums" }}
                >
                  {price.toFixed(2)}
                </text>
              </g>
            );
          })}

        {/* Price line */}
        {path && (
          <path d={path} fill="none" className="stroke-cyan" strokeWidth={1.5} />
        )}

        {/* Hover crosshair + tooltip */}
        {hoverPoint && hoverIdx !== null && (
          <g>
            <line
              x1={xScale(hoverIdx)}
              x2={xScale(hoverIdx)}
              y1={CHART_PADDING.top}
              y2={CHART_HEIGHT - CHART_PADDING.bottom}
              className="stroke-border-bright"
              strokeWidth={0.5}
              strokeDasharray="2,2"
            />
            <circle
              cx={xScale(hoverIdx)}
              cy={yScale(hoverPoint.price)}
              r={3}
              className="fill-cyan"
            />
            <text
              x={xScale(hoverIdx) + 6}
              y={yScale(hoverPoint.price) - 6}
              className="fill-text text-[10px]"
              style={{ fontVariantNumeric: "tabular-nums" }}
            >
              {hoverPoint.price.toFixed(2)} (step {hoverPoint.step})
            </text>
          </g>
        )}

        {/* Empty state */}
        {points.length === 0 && (
          <text
            x={width / 2}
            y={CHART_HEIGHT / 2}
            textAnchor="middle"
            className="fill-dim text-[11px]"
          >
            Awaiting price data...
          </text>
        )}
      </svg>
    </div>
  );
}
