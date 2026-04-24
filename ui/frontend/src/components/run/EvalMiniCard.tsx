"use client";

import Link from "next/link";

interface Props {
  runId: string;
  /** Preview of score_vector from eval_report.json (optional). */
  scores?: Record<string, number>;
}

const PREVIEW_TIERS = ["price_path", "style_facts", "microstructure"] as const;

/**
 * Score vector preview: 3 most important tier scores as mini bars.
 * Links to the full eval report.
 */
export function EvalMiniCard({ runId, scores }: Props) {
  return (
    <div className="panel">
      <h3 className="panel-title">Eval Preview</h3>
      {scores ? (
        <div className="space-y-1.5">
          {PREVIEW_TIERS.map((tier) => {
            const val = scores[tier] ?? 0;
            const pct = Math.min(val * 100, 100);
            const barColor =
              val >= 0.6
                ? "bg-bullish/50"
                : val >= 0.3
                  ? "bg-warn/50"
                  : "bg-bearish/50";
            return (
              <div key={tier} className="flex items-center gap-2 text-[10px]">
                <span className="text-dim w-28 truncate">
                  {tier.replace("_", " ")}
                </span>
                <div className="flex-1 h-2 bg-bg" aria-label={`${tier}: ${val.toFixed(2)}`}>
                  <div
                    className={`h-full ${barColor} transition-all duration-300`}
                    style={{ width: `${pct}%` }}
                  />
                </div>
                <span className="text-dim tabular-nums w-10 text-right">
                  {val.toFixed(2)}
                </span>
              </div>
            );
          })}
          <Link
            href={`/eval/${runId}`}
            className="text-[10px] text-cyan hover:underline mt-1 inline-block"
          >
            Open full eval report →
          </Link>
        </div>
      ) : (
        <div className="text-[10px]">
          <p className="text-dim mb-1">
            Eval report not yet generated for this run.
          </p>
          <Link
            href={`/eval/${runId}`}
            className="text-cyan hover:underline"
          >
            View eval page →
          </Link>
        </div>
      )}
    </div>
  );
}
