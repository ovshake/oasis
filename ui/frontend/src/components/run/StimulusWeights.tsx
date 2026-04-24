"use client";

const STIMULI = [
  { key: "price",    label: "Price" },
  { key: "news",     label: "News" },
  { key: "follow",   label: "Follow" },
  { key: "mention",  label: "Mention" },
  { key: "personal", label: "Personal" },
] as const;

interface Props {
  weights?: Record<string, number>;
}

/**
 * 5 bars showing current average stimulus weights across agents.
 * Accepts optional weights prop; shows placeholder when no data.
 */
export function StimulusWeights({ weights }: Props) {
  return (
    <div className="panel">
      <h3 className="panel-title">Stimulus Weights</h3>
      <div className="space-y-1.5">
        {STIMULI.map(({ key, label }) => {
          const val = weights?.[key] ?? 0;
          const pct = Math.min(val * 100, 100);
          return (
            <div key={key} className="flex items-center gap-2 text-[10px]">
              <span className="text-dim w-16">{label}</span>
              <div
                className="flex-1 h-2 bg-bg relative"
                aria-label={`${label}: ${(val * 100).toFixed(0)}%`}
              >
                <div
                  className="h-full bg-purple/40 transition-all duration-300"
                  style={{ width: `${pct}%` }}
                />
              </div>
              <span className="text-dim w-10 text-right tabular-nums">
                {(val * 100).toFixed(0)}%
              </span>
            </div>
          );
        })}
      </div>
      {!weights && (
        <p className="text-dim text-[10px] mt-1">
          Stimulus data available when run starts.
        </p>
      )}
    </div>
  );
}
