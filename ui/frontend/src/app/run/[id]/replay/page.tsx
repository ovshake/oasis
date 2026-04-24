"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useParams } from "next/navigation";
import { useParquet } from "@/lib/useParquet";
import { useRunStore } from "@/lib/runStore";
import type { StepData, PricePoint, AgentAction, TradeRow } from "@/lib/types";
import { SimMeta } from "@/components/run/SimMeta";
import { PersonaDist } from "@/components/run/PersonaDist";
import { TierDist } from "@/components/run/TierDist";
import { StimulusWeights } from "@/components/run/StimulusWeights";
import { PriceChart } from "@/components/run/PriceChart";
import { SocialGraph } from "@/components/run/SocialGraph";
import { OrderBook } from "@/components/run/OrderBook";
import { NewsFeed } from "@/components/run/NewsFeed";
import { AgentFeed } from "@/components/run/AgentFeed";
import { Forecast } from "@/components/run/Forecast";
import { PnlHistory } from "@/components/run/PnlHistory";
import { RecentTrades } from "@/components/run/RecentTrades";
import { EvalMiniCard } from "@/components/run/EvalMiniCard";

/**
 * Replay view — same layout as Live but driven by a step scrubber
 * reading parquet data from the backend. No WebSocket.
 */
export default function ReplayPage() {
  const params = useParams();
  const runId = params.id as string;

  const { rows: priceRows, loading: pricesLoading } = useParquet(runId, "prices");
  const { rows: actionRows, loading: actionsLoading } = useParquet(runId, "actions");
  const { rows: tradeRows, loading: tradesLoading } = useParquet(runId, "trades");

  const bulkLoad = useRunStore((s) => s.bulkLoad);
  const reset = useRunStore((s) => s.reset);
  const setStatus = useRunStore((s) => s.setStatus);

  // Scrubber state
  const [currentStep, setCurrentStep] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Compute total steps from price data
  const maxStep = useMemo(() => {
    if (priceRows.length === 0) return 0;
    return Math.max(...priceRows.map((r) => (r.step as number) ?? 0));
  }, [priceRows]);

  // Reset on mount
  useEffect(() => {
    reset();
    setStatus("completed");
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);

  // Build filtered data for current step and push to store
  const updateStore = useCallback(
    (step: number) => {
      // Prices
      const filteredPrices = priceRows.filter(
        (r) => ((r.step as number) ?? 0) <= step,
      );
      const pricesByAsset: Record<string, PricePoint[]> = {};
      for (const row of filteredPrices) {
        const asset = (row.base_symbol as string) ?? "BTC";
        if (!pricesByAsset[asset]) pricesByAsset[asset] = [];
        pricesByAsset[asset].push({
          step: (row.step as number) ?? 0,
          price: (row.last_price as number) ?? 0,
        });
      }

      // Actions
      const filteredActions = actionRows.filter(
        (r) => ((r.step as number) ?? 0) <= step,
      );
      const actions: AgentAction[] = filteredActions.map((r) => ({
        step: (r.step as number) ?? 0,
        agent_name: (r.agent_name as string) ?? "",
        archetype: (r.archetype as string) ?? "",
        action_type: (r.action_type as string) ?? "",
        details: (r.details as string) ?? "",
        follower_count: (r.follower_count as number) ?? 0,
      }));

      // Steps summary (group actions by step)
      const stepMap = new Map<number, StepData>();
      for (const a of actions) {
        if (!stepMap.has(a.step)) {
          stepMap.set(a.step, {
            step: a.step,
            total_actions: 0,
            tier_counts: {},
            archetype_counts: {},
            action_types: {},
          });
        }
        const sd = stepMap.get(a.step)!;
        sd.total_actions++;
        sd.tier_counts[a.action_type] =
          (sd.tier_counts[a.action_type] ?? 0) + 1;
        sd.archetype_counts[a.archetype] =
          (sd.archetype_counts[a.archetype] ?? 0) + 1;
        sd.action_types[a.action_type] =
          (sd.action_types[a.action_type] ?? 0) + 1;
      }

      // Cumulative counts
      const cumulativeTierCounts: Record<string, number> = {};
      const cumulativeArchetypeCounts: Record<string, number> = {};
      for (const sd of stepMap.values()) {
        for (const [k, v] of Object.entries(sd.tier_counts)) {
          cumulativeTierCounts[k] = (cumulativeTierCounts[k] ?? 0) + v;
        }
        for (const [k, v] of Object.entries(sd.archetype_counts)) {
          cumulativeArchetypeCounts[k] =
            (cumulativeArchetypeCounts[k] ?? 0) + v;
        }
      }

      // Trades
      const filteredTrades = tradeRows.filter(
        (r) => ((r.step as number) ?? 0) <= step,
      );
      const trades: TradeRow[] = filteredTrades.map((r) => ({
        step: (r.step as number) ?? 0,
        side: ((r.side as string) ?? "buy") as "buy" | "sell",
        price: (r.price as number) ?? 0,
        qty: (r.qty as number) ?? 0,
        user_name: (r.user_name as string) ?? "",
      }));

      // Determine assets
      const assets = [...new Set(Object.keys(pricesByAsset))];

      bulkLoad({
        currentStep: step,
        totalSteps: maxStep,
        prices: pricesByAsset,
        actions: actions.slice(-200),
        trades: trades.slice(-100),
        steps: Array.from(stepMap.values()),
        cumulativeTierCounts,
        cumulativeArchetypeCounts,
        assets: assets.length > 0 ? assets : ["BTC"],
      });
    },
    [priceRows, actionRows, tradeRows, maxStep, bulkLoad],
  );

  // Update store when scrubber changes or data loads
  useEffect(() => {
    if (!pricesLoading && !actionsLoading && !tradesLoading) {
      updateStore(currentStep);
    }
  }, [currentStep, pricesLoading, actionsLoading, tradesLoading, updateStore]);

  // Playback
  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current);
    if (playing && maxStep > 0) {
      timerRef.current = setInterval(() => {
        setCurrentStep((prev) => {
          const next = prev + 1;
          if (next > maxStep) {
            setPlaying(false);
            return maxStep;
          }
          return next;
        });
      }, 1000 / speed);
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [playing, speed, maxStep]);

  const loading = pricesLoading || actionsLoading || tradesLoading;

  return (
    <div className="p-2 max-w-[1800px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-2 px-1">
        <h1 className="text-[11px] font-bold tracking-wider">
          <span className="text-cyan">RUN</span>
          <span className="text-dim mx-1">/</span>
          <span className="text-purple">{runId.slice(0, 8)}</span>
          <span className="text-dim mx-1">/</span>
          <span className="text-warn">REPLAY</span>
        </h1>
        {loading && (
          <span className="text-dim text-[10px]">Loading parquet data...</span>
        )}
      </div>

      {/* Scrubber controls */}
      <div className="panel mb-2">
        <div className="flex items-center gap-3 text-[11px]">
          {/* Play/Pause */}
          <button
            onClick={() => setPlaying(!playing)}
            className="px-2 py-1 bg-cyan/10 text-cyan border border-cyan/30 hover:bg-cyan/20 transition-colors font-bold text-[10px]"
            aria-label={playing ? "Pause" : "Play"}
          >
            {playing ? "PAUSE" : "PLAY"}
          </button>

          {/* Speed buttons */}
          {[1, 10, 100].map((s) => (
            <button
              key={s}
              onClick={() => setSpeed(s)}
              className={`px-2 py-0.5 text-[10px] border transition-colors ${
                speed === s
                  ? "border-cyan/50 text-cyan bg-cyan/10"
                  : "border-border text-dim hover:border-border-bright"
              }`}
              aria-label={`Speed ${s}x`}
            >
              {s}x
            </button>
          ))}

          {/* Step scrubber */}
          <input
            type="range"
            min={0}
            max={maxStep}
            value={currentStep}
            onChange={(e) => {
              setCurrentStep(Number(e.target.value));
              setPlaying(false);
            }}
            className="flex-1 accent-cyan h-1"
            aria-label="Step scrubber"
          />

          {/* Step display */}
          <span className="text-cyan tabular-nums w-24 text-right">
            {currentStep} / {maxStep}
          </span>
        </div>
      </div>

      {/* Main grid: same layout as live view */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-2">
        <div className="space-y-2">
          <SimMeta />
          <PersonaDist />
          <TierDist />
          <StimulusWeights />
        </div>

        <div className="xl:col-span-2 space-y-2">
          <PriceChart />
          <SocialGraph />
        </div>

        <div className="space-y-2">
          <OrderBook />
          <NewsFeed />
          <AgentFeed />
          <Forecast />
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-2 mt-2">
        <PnlHistory />
        <RecentTrades />
        <EvalMiniCard runId={runId} />
      </div>
    </div>
  );
}
