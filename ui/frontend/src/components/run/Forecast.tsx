"use client";

import { useRunStore } from "@/lib/runStore";

/**
 * T+K forecast card. MVP: target, stop, confidence, direction.
 * Shows placeholder when no forecast data.
 */
export function Forecast() {
  const forecast = useRunStore((s) => s.forecast);

  return (
    <div className="panel">
      <h3 className="panel-title">Forecast</h3>
      {forecast ? (
        <div className="space-y-1.5 text-[11px]">
          <div className="flex justify-between">
            <span className="text-dim">Direction</span>
            <span
              className={`font-bold uppercase ${
                forecast.direction === "long" ? "text-bullish" : "text-bearish"
              }`}
            >
              {forecast.direction}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-dim">Target</span>
            <span className="text-bullish tabular-nums">
              {forecast.target.toFixed(2)}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-dim">Stop</span>
            <span className="text-bearish tabular-nums">
              {forecast.stop.toFixed(2)}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-dim">Confidence</span>
            <span
              className={`tabular-nums font-bold ${
                forecast.confidence >= 70
                  ? "text-bullish"
                  : forecast.confidence >= 40
                    ? "text-warn"
                    : "text-bearish"
              }`}
            >
              {forecast.confidence.toFixed(0)}%
            </span>
          </div>
        </div>
      ) : (
        <p className="text-dim text-[10px]">
          Forecast not yet available. Generated after sufficient price history.
        </p>
      )}
    </div>
  );
}
