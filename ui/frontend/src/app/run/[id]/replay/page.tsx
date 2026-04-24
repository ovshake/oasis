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
import { SocialFeed } from "@/components/run/SocialFeed";

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
  // news and conservation parquets are optional — useParquet returns []
  // (not error) if the section doesn't exist for this run.
  const { rows: newsRows } = useParquet(runId, "news");
  const { rows: conservationRows } = useParquet(runId, "conservation");
  // Tiers and stimuli power the left-rail TierDist / StimulusWeights
  // panels. tiers.parquet is the ONLY source that includes silent-tier
  // counts (silent agents skip the LLM and don't appear in actions).
  const { rows: tierRows } = useParquet(runId, "tiers");
  const { rows: stimulusRows } = useParquet(runId, "stimuli");

  const bulkLoad = useRunStore((s) => s.bulkLoad);
  const reset = useRunStore((s) => s.reset);
  const setStatus = useRunStore((s) => s.setStatus);

  // Scrubber state
  const [currentStep, setCurrentStep] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // StimulusWeights takes weights via prop, not the Zustand store. Hold
  // the computed aggregate here in local state and pass it through.
  const [stimulusWeights, setStimulusWeights] =
    useState<Record<string, number> | undefined>(undefined);

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
      // Tier distribution comes from tiers.parquet (which records ALL
      // 5 tier counts per step including silent). Using action_type
      // from actions.parquet would miss silent agents entirely since
      // they skip the LLM call and never produce an action row.
      const cumulativeTierCounts: Record<string, number> = {};
      for (const row of tierRows) {
        const s = (row.step as number) ?? 0;
        if (s > step) continue;
        const tier = (row.tier as string) ?? "";
        const cnt = (row.count as number) ?? 0;
        if (tier) {
          cumulativeTierCounts[tier] = (cumulativeTierCounts[tier] ?? 0) + cnt;
        }
      }
      const cumulativeArchetypeCounts: Record<string, number> = {};
      for (const sd of stepMap.values()) {
        for (const [k, v] of Object.entries(sd.archetype_counts)) {
          cumulativeArchetypeCounts[k] =
            (cumulativeArchetypeCounts[k] ?? 0) + v;
        }
      }

      // Stimulus weights — show the RELATIVE share of each stimulus
      // source cumulatively across the run up to currentStep. Sums to
      // 1 (shown as 100% when scaled in the panel). "News" dominates
      // on steps with news events; "Price" dominates on steps with
      // big portfolio moves. follow/mention/personal are Phase 6
      // info-filter stubs — always 0 for now.
      const stimulusTotals: Record<string, number> = {
        price: 0, news: 0, follow: 0, mention: 0, personal: 0,
      };
      for (const row of stimulusRows) {
        const s = (row.step as number) ?? 0;
        if (s > step) continue;
        stimulusTotals.price += (row.price_stimulus as number) ?? 0;
        stimulusTotals.news += (row.news_stimulus as number) ?? 0;
        stimulusTotals.follow += (row.follow_stimulus as number) ?? 0;
        stimulusTotals.mention += (row.mention_stimulus as number) ?? 0;
        stimulusTotals.personal += (row.personal_stimulus as number) ?? 0;
      }
      const totalStim =
        stimulusTotals.price +
        stimulusTotals.news +
        stimulusTotals.follow +
        stimulusTotals.mention +
        stimulusTotals.personal;
      const stimulusWeights: Record<string, number> =
        totalStim > 0
          ? {
              price: stimulusTotals.price / totalStim,
              news: stimulusTotals.news / totalStim,
              follow: stimulusTotals.follow / totalStim,
              mention: stimulusTotals.mention / totalStim,
              personal: stimulusTotals.personal / totalStim,
            }
          : {};

      // Trades — `side` is the aggressive (taker) side, derived in the
      // harness from order-id ordering. For the agent label, prefer the
      // aggressor's user_id so the tape shows who drove the trade.
      const filteredTrades = tradeRows.filter(
        (r) => ((r.step as number) ?? 0) <= step,
      );
      const trades: TradeRow[] = filteredTrades.map((r) => {
        const rawSide = r.side as string | undefined;
        const side: "buy" | "sell" =
          rawSide === "buy" || rawSide === "sell" ? rawSide : "buy";
        const buyerId = (r.buyer_id as number) ?? 0;
        const sellerId = (r.seller_id as number) ?? 0;
        const aggressorUid = side === "buy" ? buyerId : sellerId;
        return {
          step: (r.step as number) ?? 0,
          side,
          price: (r.price as number) ?? 0,
          qty: (r.qty as number) ?? 0,
          user_name: `#${aggressorUid}`,
        };
      });

      // Determine assets
      const assets = [...new Set(Object.keys(pricesByAsset))];

      // News — filter to events with step <= current. Schema from
      // news.parquet: step, source, title, sentiment_valence, audience,
      // affected_assets (JSON array as string).
      const news = newsRows
        .filter((r) => ((r.step as number) ?? 0) <= step)
        .map((r) => {
          let parsedAssets: string[] = [];
          const raw = r.affected_assets as string | string[] | undefined;
          if (Array.isArray(raw)) parsedAssets = raw;
          else if (typeof raw === "string") {
            try {
              parsedAssets = JSON.parse(raw) as string[];
            } catch {
              parsedAssets = raw.split(",").filter(Boolean);
            }
          }
          return {
            step: (r.step as number) ?? 0,
            title: (r.title as string) ?? "",
            content: (r.content as string) ?? "",
            source: (r.source as string) ?? "",
            sentiment: (r.sentiment_valence as number) ?? 0,
            assets: parsedAssets,
          };
        });

      // PnL — aggregate wealth at last_price across all instruments.
      // Conservation schema: step, instrument (or instrument_id), total_amount,
      // total_locked, total_supply. Price lookup: last_price per
      // (step, base_symbol) from priceRows.
      const priceByStepSymbol = new Map<string, number>();
      for (const row of priceRows) {
        const s = (row.step as number) ?? 0;
        const sym = (row.base_symbol as string) ?? "";
        const lp = (row.last_price as number) ?? 0;
        priceByStepSymbol.set(`${s}:${sym}`, lp);
      }
      const wealthByStep = new Map<number, number>();
      for (const row of conservationRows) {
        const s = (row.step as number) ?? 0;
        if (s > step) continue;
        const sym = (row.instrument as string) ?? (row.symbol as string) ?? "USD";
        const amt = (row.total_amount as number) ?? 0;
        const locked = (row.total_locked as number) ?? 0;
        const qty = amt + locked;
        // USD is its own unit; for base assets multiply by last_price at
        // that step (fall back to latest known price, else 1).
        let unitPrice = 1;
        if (sym !== "USD") {
          unitPrice =
            priceByStepSymbol.get(`${s}:${sym}`) ??
            priceByStepSymbol.get(`${step}:${sym}`) ??
            0;
        }
        wealthByStep.set(s, (wealthByStep.get(s) ?? 0) + qty * unitPrice);
      }
      const pnl = [...wealthByStep.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([s, w]) => ({ step: s, wealth: w }));

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
        news,
        pnl,
      });
      setStimulusWeights(
        Object.keys(stimulusWeights).length > 0 ? stimulusWeights : undefined,
      );
    },
    [
      priceRows,
      actionRows,
      tradeRows,
      newsRows,
      conservationRows,
      tierRows,
      stimulusRows,
      maxStep,
      bulkLoad,
    ],
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
          <StimulusWeights weights={stimulusWeights} />
        </div>

        <div className="xl:col-span-2 space-y-2">
          <PriceChart />
          <SocialGraph runId={runId} />
        </div>

        <div className="space-y-2">
          <OrderBook runId={runId} />
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

      <div className="mt-2">
        <SocialFeed runId={runId} showComments={true} maxHeight="max-h-[600px]" />
      </div>
    </div>
  );
}
