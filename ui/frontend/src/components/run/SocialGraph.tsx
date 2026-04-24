"use client";

import { useRunStore } from "@/lib/runStore";

/**
 * Simplified MVP: grouped list of top 20 most-recent posters
 * (from last 10 steps). Rows: archetype badge, name, follower count, step.
 * Grouped by archetype then sorted by follower count.
 *
 * Full D3-force viz is deferred to phase-2.
 */
export function SocialGraph() {
  const recentPosters = useRunStore((s) => s.recentPosters);

  // Group by archetype
  const grouped = new Map<string, typeof recentPosters>();
  for (const p of recentPosters) {
    const list = grouped.get(p.archetype) ?? [];
    list.push(p);
    grouped.set(p.archetype, list);
  }

  const archetypes = Array.from(grouped.keys()).sort();

  return (
    <div className="panel">
      <h3 className="panel-title">Social Feed (Recent Posters)</h3>
      {recentPosters.length === 0 ? (
        <p className="text-dim text-[10px]">No posts yet. Waiting for agent activity...</p>
      ) : (
        <div className="space-y-2 max-h-48 overflow-y-auto">
          {archetypes.map((arch) => (
            <div key={arch}>
              <div className="text-[9px] text-dim uppercase tracking-widest mb-0.5">
                {arch.replace("_", " ")}
              </div>
              {(grouped.get(arch) ?? []).map((p) => (
                <div
                  key={`${p.agent_name}-${p.step}`}
                  className="flex items-center gap-2 text-[10px] py-0.5"
                >
                  <span className="px-1 py-0.5 bg-purple/10 text-purple text-[9px] font-bold">
                    {arch.slice(0, 3).toUpperCase()}
                  </span>
                  <span className="text-purple flex-1 truncate">{p.agent_name}</span>
                  <span className="text-cyan tabular-nums">{p.follower_count}</span>
                  <span className="text-dim tabular-nums text-[9px]">s{p.step}</span>
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
