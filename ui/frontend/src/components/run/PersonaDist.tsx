"use client";

import { useRunStore } from "@/lib/runStore";

const ARCHETYPES = [
  "lurker", "hodler", "paperhands", "fomo_degen", "ta",
  "contrarian", "news_trader", "whale", "kol", "market_maker",
] as const;

/** 10 horizontal bars showing counts per archetype. */
export function PersonaDist() {
  const counts = useRunStore((s) => s.cumulativeArchetypeCounts);
  const total = Object.values(counts).reduce((a, b) => a + b, 0);

  return (
    <div className="panel">
      <h3 className="panel-title">Persona Distribution</h3>
      <div className="space-y-1">
        {ARCHETYPES.map((arch) => {
          const count = counts[arch] ?? 0;
          const pct = total > 0 ? (count / total) * 100 : 0;
          return (
            <div key={arch} className="flex items-center gap-2 text-[10px]">
              <span className="text-cyan w-24 truncate">
                {arch.replace("_", " ")}
              </span>
              <div className="flex-1 h-2 bg-bg relative" aria-label={`${arch}: ${count}`}>
                <div
                  className="h-full bg-cyan/40 transition-all duration-300"
                  style={{ width: `${Math.min(pct, 100)}%` }}
                />
              </div>
              <span className="text-dim w-10 text-right tabular-nums">
                {count}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
