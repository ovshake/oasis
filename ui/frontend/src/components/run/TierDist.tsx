"use client";

import { useRunStore } from "@/lib/runStore";

const TIERS: { key: string; label: string; colorClass: string }[] = [
  { key: "silent",  label: "Silent",  colorClass: "bg-dim/40" },
  { key: "react",   label: "React",   colorClass: "bg-cyan/50" },
  { key: "comment", label: "Comment", colorClass: "bg-text/40" },
  { key: "post",    label: "Post",    colorClass: "bg-warn/50" },
  { key: "trade",   label: "Trade",   colorClass: "bg-purple/50" },
];

/** 5 horizontal bars showing action tier distribution with running %. */
export function TierDist() {
  const counts = useRunStore((s) => s.cumulativeTierCounts);
  const total = Object.values(counts).reduce((a, b) => a + b, 0);

  return (
    <div className="panel">
      <h3 className="panel-title">Tier Distribution</h3>
      <div className="space-y-1.5">
        {TIERS.map(({ key, label, colorClass }) => {
          const count = counts[key] ?? 0;
          const pct = total > 0 ? (count / total) * 100 : 0;
          return (
            <div key={key} className="flex items-center gap-2 text-[10px]">
              <span className="text-dim w-16">{label}</span>
              <div
                className="flex-1 h-2.5 bg-bg relative"
                aria-label={`${label}: ${pct.toFixed(1)}%`}
              >
                <div
                  className={`h-full ${colorClass} transition-all duration-300`}
                  style={{ width: `${Math.min(pct, 100)}%` }}
                />
              </div>
              <span className="text-dim w-12 text-right tabular-nums">
                {pct.toFixed(1)}%
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
