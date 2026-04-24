"use client";

import { useEffect, useRef } from "react";
import { useRunStore } from "@/lib/runStore";

/** Live stream of agent actions. Auto-scrolls, capped at 200 rows. */
export function AgentFeed() {
  const actions = useRunStore((s) => s.actions);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [actions.length]);

  return (
    <div className="panel">
      <h3 className="panel-title">Agent Feed</h3>
      {actions.length === 0 ? (
        <p className="text-dim text-[10px]">Awaiting agent actions...</p>
      ) : (
        <div className="max-h-48 overflow-y-auto text-[10px]">
          {actions.map((a, i) => (
            <div
              key={`${a.step}-${a.agent_name}-${i}`}
              className="flex items-center gap-1.5 py-0.5 border-b border-border/30 last:border-0"
            >
              <span className="text-dim tabular-nums shrink-0 w-8">
                s{a.step}
              </span>
              <span className="text-purple truncate max-w-[100px]" title={a.agent_name}>
                {a.agent_name}
              </span>
              <ActionChip type={a.action_type} />
              <span className="text-dim truncate flex-1" title={a.details}>
                {a.details}
              </span>
            </div>
          ))}
          <div ref={bottomRef} />
        </div>
      )}
    </div>
  );
}

function ActionChip({ type }: { type: string }) {
  const colorMap: Record<string, string> = {
    trade: "text-purple bg-purple/10",
    post: "text-warn bg-warn/10",
    react: "text-cyan bg-cyan/10",
    comment: "text-text bg-text/10",
    silent: "text-dim bg-dim/10",
  };
  const cls = colorMap[type] ?? "text-dim bg-dim/10";
  return (
    <span className={`px-1 py-0.5 text-[9px] font-bold shrink-0 ${cls}`}>
      {type.toUpperCase()}
    </span>
  );
}
