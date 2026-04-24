"use client";

import { useCallback, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { api, type Scenario, type ManualNewsEvent, type PopulationMix } from "@/lib/api";

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const ALL_ASSETS = ["BTC", "ETH", "USDT", "XAU", "WTI", "USD"] as const;

const ARCHETYPES: (keyof PopulationMix)[] = [
  "lurker", "hodler", "paperhands", "fomo_degen", "ta",
  "contrarian", "news_trader", "whale", "kol", "market_maker",
];

const DEFAULT_MIX: PopulationMix = {
  lurker: 0.45, hodler: 0.15, paperhands: 0.15, fomo_degen: 0.08,
  ta: 0.05, contrarian: 0.03, news_trader: 0.04, whale: 0.01,
  kol: 0.02, market_maker: 0.02,
};

const EMPTY_NEWS: ManualNewsEvent = {
  step: 0, content: "", sentiment: 0, assets: [], audience: "all",
};

type PriceSource = "default" | "live" | "historical" | "manual";
type NewsKind = "manual" | "historical" | "live_snapshot";

interface FormState {
  name: string;
  duration_steps: number;
  step_minutes: number;
  seed: number;
  agents_count: number;
  assets: string[];
  price_source: PriceSource;
  as_of_date: string;
  initial_prices: Record<string, number>;
  news_kind: NewsKind;
  manual_events: ManualNewsEvent[];
  mix: PopulationMix;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

interface Props {
  initial?: Partial<Scenario>;
  isNew?: boolean;
}

export function ScenarioForm({ initial, isNew }: Props) {
  const router = useRouter();

  const [form, setForm] = useState<FormState>({
    name: initial?.name ?? "",
    duration_steps: initial?.duration_steps ?? 240,
    step_minutes: initial?.step_minutes ?? 1,
    seed: initial?.seed ?? 42,
    agents_count: initial?.agents_count ?? 1000,
    assets: initial?.assets ?? [...ALL_ASSETS],
    price_source: initial?.price_source ?? "default",
    as_of_date: initial?.as_of_date ?? "",
    initial_prices: initial?.initial_prices ?? {},
    news_kind: initial?.news_source?.kind ?? "manual",
    manual_events: initial?.manual_events ?? [],
    mix: initial?.population_mix ?? { ...DEFAULT_MIX },
  });

  const [showYaml, setShowYaml] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // -- Mix sum validation --
  const mixSum = useMemo(() => {
    let s = 0;
    for (const k of ARCHETYPES) s += form.mix[k];
    return s;
  }, [form.mix]);

  const mixValid = Math.abs(mixSum - 1.0) < 0.005;

  // -- Asset toggle --
  const toggleAsset = useCallback((sym: string) => {
    setForm((prev) => {
      const has = prev.assets.includes(sym);
      return {
        ...prev,
        assets: has
          ? prev.assets.filter((a) => a !== sym)
          : [...prev.assets, sym],
      };
    });
  }, []);

  // -- Mix slider --
  const setMixValue = useCallback((key: keyof PopulationMix, val: number) => {
    setForm((prev) => ({ ...prev, mix: { ...prev.mix, [key]: val } }));
  }, []);

  // -- Manual news helpers --
  const addNewsRow = useCallback(() => {
    setForm((prev) => ({
      ...prev,
      manual_events: [...prev.manual_events, { ...EMPTY_NEWS }],
    }));
  }, []);

  const removeNewsRow = useCallback((idx: number) => {
    setForm((prev) => ({
      ...prev,
      manual_events: prev.manual_events.filter((_, i) => i !== idx),
    }));
  }, []);

  const updateNewsRow = useCallback(
    (idx: number, field: keyof ManualNewsEvent, value: unknown) => {
      setForm((prev) => ({
        ...prev,
        manual_events: prev.manual_events.map((row, i) =>
          i === idx ? { ...row, [field]: value } : row
        ),
      }));
    },
    []
  );

  // -- Manual price helpers --
  const setInitialPrice = useCallback((sym: string, val: number) => {
    setForm((prev) => ({
      ...prev,
      initial_prices: { ...prev.initial_prices, [sym]: val },
    }));
  }, []);

  // -- Build payload --
  const buildPayload = useCallback((): Record<string, unknown> => {
    return {
      name: form.name,
      duration_steps: form.duration_steps,
      step_minutes: form.step_minutes,
      seed: form.seed,
      agents_count: form.agents_count,
      assets: form.assets,
      price_source: form.price_source,
      as_of_date: form.as_of_date || null,
      initial_prices: form.price_source === "manual" ? form.initial_prices : {},
      population_mix: form.mix,
      news_source: { kind: form.news_kind, providers: [], enrich_with: "mock" },
      manual_events: form.news_kind === "manual" ? form.manual_events : [],
    };
  }, [form]);

  // -- YAML preview --
  const yamlPreview = useMemo(() => {
    const payload = buildPayload();
    // Simplified YAML-like serialization (no dependency needed for MVP)
    return JSON.stringify(payload, null, 2);
  }, [buildPayload]);

  // -- Save --
  const handleSave = useCallback(async () => {
    if (!form.name.trim()) {
      setError("Scenario name is required.");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await api.saveScenario(buildPayload());
      if (isNew) {
        router.push(`/scenarios/${form.name}`);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }, [form.name, buildPayload, isNew, router]);

  // -- Run --
  const handleRun = useCallback(async () => {
    if (!mixValid) return;
    if (!form.name.trim()) {
      setError("Scenario name is required.");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await api.saveScenario(buildPayload());
      const { run_id } = await api.startRun(form.name, [form.seed], false);
      router.push(`/run/${run_id}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }, [form.name, form.seed, mixValid, buildPayload, router]);

  // -- Render --
  return (
    <div className="p-4 max-w-5xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-bold tracking-wider">
          <span className="text-cyan">SCENARIO</span>
          <span className="text-dim mx-2">/</span>
          <span className="text-text">
            {isNew ? "NEW" : form.name.toUpperCase() || "EDITOR"}
          </span>
        </h1>
        <div className="flex gap-2">
          <button
            onClick={handleSave}
            disabled={saving}
            className="px-3 py-1.5 text-[11px] font-bold uppercase tracking-wider
                       bg-cyan/10 text-cyan border border-cyan/30
                       hover:bg-cyan/20 disabled:opacity-40 transition-colors"
          >
            {saving ? "Saving..." : "Save"}
          </button>
          <button
            onClick={handleRun}
            disabled={saving || !mixValid}
            className="px-3 py-1.5 text-[11px] font-bold uppercase tracking-wider
                       bg-bullish/10 text-bullish border border-bullish/30
                       hover:bg-bullish/20 disabled:opacity-40 transition-colors"
          >
            {saving ? "Starting..." : "Run"}
          </button>
        </div>
      </div>

      {/* Error banner */}
      {error && (
        <div className="panel border-bearish/50 text-bearish text-[11px]">
          {error}
        </div>
      )}

      {/* Mix warning */}
      {!mixValid && (
        <div className="panel border-warn/50 text-warn text-[11px]">
          Population mix sums to {(mixSum * 100).toFixed(1)}% — must be 100%.
          Adjust sliders before running.
        </div>
      )}

      {/* Basic fields */}
      <section className="panel space-y-3">
        <h2 className="panel-title">Configuration</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <Field label="Name">
            <input
              type="text"
              value={form.name}
              onChange={(e) => setForm((p) => ({ ...p, name: e.target.value }))}
              placeholder="my_scenario"
              className="input-field"
            />
          </Field>
          <Field label="Duration (steps)">
            <input
              type="number"
              value={form.duration_steps}
              onChange={(e) =>
                setForm((p) => ({ ...p, duration_steps: Number(e.target.value) }))
              }
              min={1}
              className="input-field"
            />
          </Field>
          <Field label="Step (min)">
            <input
              type="number"
              value={form.step_minutes}
              onChange={(e) =>
                setForm((p) => ({ ...p, step_minutes: Number(e.target.value) }))
              }
              min={1}
              className="input-field"
            />
          </Field>
          <Field label="Seed">
            <input
              type="number"
              value={form.seed}
              onChange={(e) =>
                setForm((p) => ({ ...p, seed: Number(e.target.value) }))
              }
              className="input-field"
            />
          </Field>
          <Field label="Agents">
            <input
              type="number"
              value={form.agents_count}
              onChange={(e) =>
                setForm((p) => ({ ...p, agents_count: Number(e.target.value) }))
              }
              min={1}
              className="input-field"
            />
          </Field>
        </div>
      </section>

      {/* Assets */}
      <section className="panel">
        <h2 className="panel-title">Assets</h2>
        <div className="flex flex-wrap gap-2">
          {ALL_ASSETS.map((sym) => {
            const on = form.assets.includes(sym);
            return (
              <button
                key={sym}
                onClick={() => toggleAsset(sym)}
                className={`px-2 py-1 text-[11px] font-bold border transition-colors ${
                  on
                    ? "border-cyan/50 text-cyan bg-cyan/10"
                    : "border-border text-dim bg-transparent hover:border-border-bright"
                }`}
              >
                {sym}
              </button>
            );
          })}
        </div>
      </section>

      {/* Price source */}
      <section className="panel space-y-3">
        <h2 className="panel-title">Price Source</h2>
        <div className="flex flex-wrap gap-3 text-[11px]">
          {(["default", "live", "historical", "manual"] as PriceSource[]).map(
            (opt) => (
              <label key={opt} className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="radio"
                  name="price_source"
                  checked={form.price_source === opt}
                  onChange={() => setForm((p) => ({ ...p, price_source: opt }))}
                  className="accent-cyan"
                />
                <span className={form.price_source === opt ? "text-text" : "text-dim"}>
                  {opt.toUpperCase()}
                </span>
              </label>
            )
          )}
        </div>
        {form.price_source === "historical" && (
          <Field label="As-of Date">
            <input
              type="datetime-local"
              value={form.as_of_date}
              onChange={(e) => setForm((p) => ({ ...p, as_of_date: e.target.value }))}
              className="input-field"
            />
          </Field>
        )}
        {form.price_source === "manual" && (
          <div className="space-y-1">
            <p className="text-[10px] text-dim uppercase tracking-wider mb-1">
              Initial Prices (USD)
            </p>
            {form.assets.map((sym) => (
              <div key={sym} className="flex items-center gap-2">
                <span className="text-[11px] w-12 text-dim">{sym}</span>
                <input
                  type="number"
                  value={form.initial_prices[sym] ?? ""}
                  onChange={(e) => setInitialPrice(sym, Number(e.target.value))}
                  placeholder="0.00"
                  step="0.01"
                  className="input-field w-32"
                />
              </div>
            ))}
          </div>
        )}
      </section>

      {/* News source */}
      <section className="panel space-y-3">
        <h2 className="panel-title">News Source</h2>
        <div className="flex flex-wrap gap-3 text-[11px]">
          {(["manual", "historical", "live_snapshot"] as NewsKind[]).map(
            (opt) => (
              <label key={opt} className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="radio"
                  name="news_kind"
                  checked={form.news_kind === opt}
                  onChange={() => setForm((p) => ({ ...p, news_kind: opt }))}
                  className="accent-cyan"
                />
                <span className={form.news_kind === opt ? "text-text" : "text-dim"}>
                  {opt.toUpperCase().replace("_", " ")}
                </span>
              </label>
            )
          )}
        </div>
        {form.news_kind === "manual" && (
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <p className="text-[10px] text-dim uppercase tracking-wider">
                Timeline Events
              </p>
              <button
                onClick={addNewsRow}
                className="text-[10px] text-cyan hover:underline"
              >
                + Add Event
              </button>
            </div>
            {form.manual_events.length === 0 && (
              <p className="text-dim text-[11px]">No events. Click + Add Event.</p>
            )}
            {form.manual_events.map((ev, i) => (
              <div
                key={i}
                className="grid grid-cols-[60px_1fr_80px_100px_auto] gap-2 items-start text-[11px]"
              >
                <input
                  type="number"
                  value={ev.step}
                  onChange={(e) => updateNewsRow(i, "step", Number(e.target.value))}
                  placeholder="Step"
                  className="input-field"
                />
                <input
                  type="text"
                  value={ev.content}
                  onChange={(e) => updateNewsRow(i, "content", e.target.value)}
                  placeholder="Event content..."
                  className="input-field"
                />
                <input
                  type="number"
                  value={ev.sentiment}
                  onChange={(e) =>
                    updateNewsRow(i, "sentiment", Number(e.target.value))
                  }
                  placeholder="Sent."
                  step="0.1"
                  min={-1}
                  max={1}
                  className="input-field"
                />
                <input
                  type="text"
                  value={ev.assets.join(",")}
                  onChange={(e) =>
                    updateNewsRow(
                      i,
                      "assets",
                      e.target.value.split(",").map((s) => s.trim()).filter(Boolean)
                    )
                  }
                  placeholder="BTC,ETH"
                  className="input-field"
                />
                <button
                  onClick={() => removeNewsRow(i)}
                  className="text-bearish hover:underline text-[10px] mt-1"
                >
                  DEL
                </button>
              </div>
            ))}
          </div>
        )}
      </section>

      {/* Population mix */}
      <section className="panel space-y-3">
        <h2 className="panel-title">Population Mix</h2>
        <div className="space-y-2">
          {ARCHETYPES.map((arch) => (
            <div key={arch} className="flex items-center gap-3">
              <span className="text-[11px] text-dim w-28 truncate">
                {arch.replace("_", " ")}
              </span>
              <input
                type="range"
                min={0}
                max={100}
                step={1}
                value={Math.round(form.mix[arch] * 100)}
                onChange={(e) =>
                  setMixValue(arch, Number(e.target.value) / 100)
                }
                className="flex-1 accent-cyan h-1"
              />
              <span className="text-[11px] font-mono w-14 text-right tabular-nums">
                {(form.mix[arch] * 100).toFixed(1)}%
              </span>
            </div>
          ))}
          <div className="flex justify-end pt-1 border-t border-border">
            <span
              className={`text-[11px] font-bold tabular-nums ${
                mixValid ? "text-bullish" : "text-warn"
              }`}
            >
              Total: {(mixSum * 100).toFixed(1)}%
            </span>
          </div>
        </div>
      </section>

      {/* YAML preview */}
      <section className="panel">
        <button
          onClick={() => setShowYaml(!showYaml)}
          className="panel-title cursor-pointer hover:text-text transition-colors w-full text-left"
        >
          {showYaml ? "[-]" : "[+]"} Raw Config Preview
        </button>
        {showYaml && (
          <pre className="mt-2 p-2 bg-bg border border-border text-[10px] text-dim overflow-auto max-h-96">
            {yamlPreview}
          </pre>
        )}
      </section>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function Field({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block space-y-1">
      <span className="text-[10px] text-dim uppercase tracking-wider">
        {label}
      </span>
      {children}
    </label>
  );
}
