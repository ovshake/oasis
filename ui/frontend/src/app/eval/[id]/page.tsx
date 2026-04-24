import type { EvalReport, MetricRow } from "@/lib/types";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

// ---------------------------------------------------------------------------
// Data fetch (server component)
// ---------------------------------------------------------------------------

async function fetchEvalReport(runId: string): Promise<EvalReport | null> {
  try {
    const res = await fetch(`${API_BASE}/api/runs/${runId}/eval`, {
      cache: "no-store",
    });
    if (!res.ok) return null;
    return (await res.json()) as EvalReport;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Score tier mapping (matches report.py TIERS + baselines)
// ---------------------------------------------------------------------------

const ALL_SCORE_KEYS = [
  { key: "price_path",       label: "A -- Price Path" },
  { key: "style_facts",      label: "B -- Style Facts" },
  { key: "microstructure",   label: "C -- Microstructure" },
  { key: "cross_asset",      label: "D -- Cross-Asset" },
  { key: "social",           label: "E -- Social" },
  { key: "agent_level",      label: "F -- Agent-Level" },
  { key: "vs_random_walk",   label: "vs Random Walk" },
  { key: "vs_no_news",       label: "vs No-News" },
] as const;

// Group metrics by tier name prefix (simplified mapping)
const TIER_METRIC_MAP: Record<string, string[]> = {
  price_path: [
    "direction_match_pct", "peak_drawdown_error", "drawdown_timing_error",
    "path_correlation", "terminal_price_error",
  ],
  style_facts: [
    "return_kurtosis", "vol_clustering_acf", "realized_vol", "green_red_ratio",
  ],
  microstructure: ["active_agent_rate", "trade_size_distribution"],
  cross_asset: ["correlation_frobenius_distance"],
  social: ["post_volume_around_news", "sentiment_price_correlation"],
  agent_level: ["conservation_check", "gini_wealth"],
};

function getMetricsForTier(
  tierKey: string,
  metrics: MetricRow[],
): MetricRow[] {
  const names = TIER_METRIC_MAP[tierKey] ?? [];
  if (names.length === 0) {
    // Fallback: return metrics whose name contains the tier key
    return metrics.filter((m) =>
      m.name.toLowerCase().includes(tierKey.replace("_", "")),
    );
  }
  return metrics.filter((m) => names.includes(m.name));
}

// ---------------------------------------------------------------------------
// Components
// ---------------------------------------------------------------------------

function ScoreBar({ value, label }: { value: number; label: string }) {
  const pct = Math.min(value * 100, 100);
  const barColor =
    value >= 0.6
      ? "bg-bullish/50"
      : value >= 0.3
        ? "bg-warn/50"
        : "bg-bearish/50";
  const textColor =
    value >= 0.6
      ? "text-bullish"
      : value >= 0.3
        ? "text-warn"
        : "text-bearish";
  const passLabel =
    value >= 0.6 ? "PASS" : value >= 0.3 ? "WARN" : "FAIL";
  const passColor =
    value >= 0.6
      ? "text-bullish bg-bullish/10"
      : value >= 0.3
        ? "text-warn bg-warn/10"
        : "text-bearish bg-bearish/10";

  return (
    <div className="flex items-center gap-2 text-[11px]">
      <span className="text-dim w-40 truncate">{label}</span>
      <div
        className="flex-1 h-3 bg-bg"
        aria-label={`${label}: ${value.toFixed(2)}`}
      >
        <div
          className={`h-full ${barColor} transition-all duration-300`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className={`tabular-nums w-10 text-right ${textColor}`}>
        {value.toFixed(2)}
      </span>
      <span
        className={`px-1.5 py-0.5 text-[9px] font-bold ${passColor}`}
      >
        {passLabel}
      </span>
    </div>
  );
}

function MetricTable({
  metrics,
  baselines,
}: {
  metrics: MetricRow[];
  baselines: Record<string, MetricRow[]>;
}) {
  if (metrics.length === 0) {
    return (
      <p className="text-dim text-[10px] py-2">
        No metrics computed for this tier.
      </p>
    );
  }

  // Collect baseline values keyed by metric name
  const baselineByName: Record<string, Record<string, number>> = {};
  for (const [blName, blMetrics] of Object.entries(baselines)) {
    for (const m of blMetrics) {
      if (!baselineByName[m.name]) baselineByName[m.name] = {};
      baselineByName[m.name][blName] = m.value;
    }
  }

  const blNames = Object.keys(baselines);

  return (
    <div className="overflow-x-auto text-[10px]">
      <table className="w-full">
        <thead>
          <tr className="text-dim text-[9px] uppercase tracking-widest text-left">
            <th className="py-1 pr-3">Metric</th>
            <th className="py-1 pr-3">Sim Value</th>
            <th className="py-1 pr-3">Threshold</th>
            {blNames.map((bl) => (
              <th key={bl} className="py-1 pr-3">
                {bl}
              </th>
            ))}
            <th className="py-1">Pass</th>
          </tr>
        </thead>
        <tbody>
          {metrics.map((m) => (
            <tr
              key={m.name}
              className="border-t border-border/50"
            >
              <td className="py-1 pr-3 text-text">{m.name}</td>
              <td className="py-1 pr-3 tabular-nums text-cyan">
                {isFinite(m.value) ? m.value.toFixed(4) : "NaN"}
                {m.ci_low !== undefined && m.ci_high !== undefined && (
                  <span className="text-dim ml-1">
                    [{m.ci_low.toFixed(4)}, {m.ci_high.toFixed(4)}]
                  </span>
                )}
              </td>
              <td className="py-1 pr-3 tabular-nums text-dim">
                {m.threshold !== null ? m.threshold.toFixed(4) : "--"}
              </td>
              {blNames.map((bl) => (
                <td key={bl} className="py-1 pr-3 tabular-nums text-dim">
                  {baselineByName[m.name]?.[bl] !== undefined
                    ? baselineByName[m.name][bl].toFixed(4)
                    : "--"}
                </td>
              ))}
              <td className="py-1">
                <PassBadge passed={m.passed} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function PassBadge({ passed }: { passed: boolean | null }) {
  if (passed === true) {
    return (
      <span className="px-1.5 py-0.5 text-[9px] font-bold text-bullish bg-bullish/10">
        PASS
      </span>
    );
  }
  if (passed === false) {
    return (
      <span className="px-1.5 py-0.5 text-[9px] font-bold text-bearish bg-bearish/10">
        FAIL
      </span>
    );
  }
  return <span className="text-dim text-[9px]">--</span>;
}

function CollapsibleTier({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <details className="panel group" open>
      <summary className="panel-title cursor-pointer select-none hover:text-text transition-colors">
        <span className="group-open:before:content-['[-]'] before:content-['[+]'] before:text-dim before:mr-1" />
        {label}
      </summary>
      <div className="mt-2">{children}</div>
    </details>
  );
}

// ---------------------------------------------------------------------------
// Price overlay SVG chart
// ---------------------------------------------------------------------------

function PriceOverlayChart({
  simPrices,
}: {
  simPrices: number[];
}) {
  if (simPrices.length === 0) {
    return (
      <div className="panel">
        <h3 className="panel-title">Price Overlay</h3>
        <p className="text-dim text-[10px]">No price data available for overlay chart.</p>
      </div>
    );
  }

  const W = 600;
  const H = 200;
  const PAD = { top: 12, right: 50, bottom: 20, left: 8 };
  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;

  const mn = Math.min(...simPrices);
  const mx = Math.max(...simPrices);
  const range = mx - mn || 1;

  const xs = (i: number) =>
    PAD.left + (i / Math.max(simPrices.length - 1, 1)) * innerW;
  const ys = (p: number) =>
    PAD.top + innerH - ((p - mn) / range) * innerH;

  const simPath = simPrices
    .map(
      (p, i) =>
        `${i === 0 ? "M" : "L"}${xs(i).toFixed(1)},${ys(p).toFixed(1)}`,
    )
    .join(" ");

  return (
    <div className="panel">
      <h3 className="panel-title">Price Overlay</h3>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full"
        style={{ height: H }}
        aria-label="Simulated price overlay chart"
      >
        {/* Y-axis labels */}
        {[0, 0.5, 1].map((f) => {
          const price = mn + range * f;
          const y = ys(price);
          return (
            <g key={f}>
              <line
                x1={PAD.left}
                x2={W - PAD.right}
                y1={y}
                y2={y}
                className="stroke-border"
                strokeWidth={0.5}
              />
              <text
                x={W - PAD.right + 4}
                y={y + 3}
                className="fill-dim text-[8px]"
                style={{ fontVariantNumeric: "tabular-nums" }}
              >
                {price.toFixed(2)}
              </text>
            </g>
          );
        })}

        {/* Sim line */}
        <path
          d={simPath}
          fill="none"
          className="stroke-cyan"
          strokeWidth={1.5}
        />

        {/* Legend */}
        <g transform={`translate(${PAD.left + 4}, ${PAD.top + 10})`}>
          <line x1={0} x2={16} y1={0} y2={0} className="stroke-cyan" strokeWidth={1.5} />
          <text x={20} y={3} className="fill-dim text-[8px]">Simulated</text>
        </g>
      </svg>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Page (server component)
// ---------------------------------------------------------------------------

export default async function EvalReportPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id: runId } = await params;
  const report = await fetchEvalReport(runId);

  if (!report) {
    return (
      <div className="p-4 max-w-5xl mx-auto">
        <div className="panel text-center py-8">
          <h2 className="text-cyan text-sm font-bold mb-2">
            Eval Report Not Available
          </h2>
          <p className="text-dim text-[11px]">
            No eval_report.json found for run {runId.slice(0, 8)}.
            The eval may not have been generated yet.
          </p>
        </div>
      </div>
    );
  }

  // Extract sim prices from metrics if present (for the overlay chart)
  // In practice, the price overlay reads from parquet. For MVP, we pass empty.
  const simPrices: number[] = [];

  return (
    <div className="p-4 max-w-6xl mx-auto space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-[11px] font-bold tracking-wider">
          <span className="text-cyan">EVAL</span>
          <span className="text-dim mx-1">/</span>
          <span className="text-purple">{runId.slice(0, 8)}</span>
        </h1>
        <div className="text-dim text-[10px] flex gap-4">
          <span>Mode: {report.mode.toUpperCase()}</span>
          <span>Generated: {report.generated_at}</span>
        </div>
      </div>

      {/* Score vector */}
      <section className="panel space-y-2">
        <h2 className="panel-title">Score Vector</h2>
        {ALL_SCORE_KEYS.map(({ key, label }) => (
          <ScoreBar
            key={key}
            value={report.score_vector[key] ?? 0}
            label={label}
          />
        ))}
      </section>

      {/* Per-tier metric tables */}
      {ALL_SCORE_KEYS.filter(({ key }) => !key.startsWith("vs_")).map(
        ({ key, label }) => {
          const tierMetrics = getMetricsForTier(key, report.metrics);
          return (
            <CollapsibleTier key={key} label={label}>
              <MetricTable
                metrics={tierMetrics}
                baselines={report.baselines}
              />
            </CollapsibleTier>
          );
        },
      )}

      {/* Price overlay chart */}
      <PriceOverlayChart simPrices={simPrices} />

      {/* Caveats */}
      {report.caveats.length > 0 && (
        <section className="panel">
          <h2 className="panel-title">Caveats</h2>
          <ul className="text-dim text-[10px] space-y-0.5 list-disc list-inside">
            {report.caveats.map((c, i) => (
              <li key={i}>{c}</li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}
