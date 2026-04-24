"use client";

import { useRunStore } from "@/lib/runStore";

/** Scenario name, seed, step N/total, status chip, elapsed ms. */
export function SimMeta() {
  const scenarioName = useRunStore((s) => s.scenarioName);
  const seed = useRunStore((s) => s.seed);
  const currentStep = useRunStore((s) => s.currentStep);
  const totalSteps = useRunStore((s) => s.totalSteps);
  const status = useRunStore((s) => s.status);
  const elapsedMs = useRunStore((s) => s.elapsedMs);

  const statusColor: Record<string, string> = {
    connecting: "text-dim",
    running: "text-cyan",
    completed: "text-bullish",
    error: "text-bearish",
  };

  return (
    <div className="panel">
      <h3 className="panel-title">
        <span className="live-dot">Simulation</span>
      </h3>
      <div className="space-y-1.5 text-[11px]">
        <Row label="Scenario" value={scenarioName || "--"} />
        <Row label="Seed" value={String(seed)} />
        <Row
          label="Step"
          value={
            totalSteps
              ? `${currentStep} / ${totalSteps}`
              : String(currentStep)
          }
          valueClass="text-cyan tabular-nums"
        />
        <Row
          label="Status"
          value={status.toUpperCase()}
          valueClass={`font-bold uppercase ${statusColor[status] ?? "text-dim"}`}
        />
        <Row
          label="Elapsed"
          value={elapsedMs > 0 ? `${(elapsedMs / 1000).toFixed(1)}s` : "--"}
          valueClass="text-dim tabular-nums"
        />
      </div>
    </div>
  );
}

function Row({
  label,
  value,
  valueClass,
}: {
  label: string;
  value: string;
  valueClass?: string;
}) {
  return (
    <div className="flex justify-between">
      <span className="text-dim">{label}</span>
      <span className={valueClass ?? "text-text"}>{value}</span>
    </div>
  );
}
