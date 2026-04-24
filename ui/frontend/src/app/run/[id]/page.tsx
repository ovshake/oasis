"use client";

import { useEffect, useMemo } from "react";
import { useParams } from "next/navigation";
import { useRunTelemetry } from "@/lib/useRunTelemetry";
import { useRunStore } from "@/lib/runStore";
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
import { GodMode } from "@/components/run/GodMode";
import { PnlHistory } from "@/components/run/PnlHistory";
import { RecentTrades } from "@/components/run/RecentTrades";
import { EvalMiniCard } from "@/components/run/EvalMiniCard";

/**
 * Live run view — connected via WebSocket to /ws/runs/{id}.
 *
 * Layout: 4-column grid on xl, collapses to stacked on smaller screens.
 * Left: sim meta + distributions.
 * Center: price chart + social graph.
 * Right: order book + feeds + forecast.
 * Bottom: PnL + trades + eval mini card.
 */
export default function LiveRunPage() {
  const params = useParams();
  const runId = params.id as string;

  const { steps, status, error, totalSteps, elapsedMs } =
    useRunTelemetry(runId);

  const pushStep = useRunStore((s) => s.pushStep);
  const setStatus = useRunStore((s) => s.setStatus);
  const setTotalSteps = useRunStore((s) => s.setTotalSteps);
  const setElapsedMs = useRunStore((s) => s.setElapsedMs);
  const reset = useRunStore((s) => s.reset);
  const storeSteps = useRunStore((s) => s.steps);
  const actions = useRunStore((s) => s.actions);

  // Derive recentActors: user_ids from the last 3 steps of actions
  const recentActors = useMemo(() => {
    if (storeSteps.length === 0) return [];
    const currentStep = storeSteps[storeSteps.length - 1]?.step ?? 0;
    const cutoff = currentStep - 3;
    const ids = new Set<number>();
    for (const a of actions) {
      if (a.step >= cutoff && a.follower_count !== undefined) {
        // follower_count as a proxy for user_id presence isn't ideal,
        // but we extract what we can from the action feed.
        ids.add(a.step * 1000 + (a.follower_count ?? 0));
      }
    }
    // In practice, actions don't carry user_id directly in the WS shape.
    // Return empty until the telemetry shape includes user_id.
    return [] as number[];
  }, [storeSteps, actions]);

  // Reset store on mount, sync WS data into store
  useEffect(() => {
    reset();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);

  useEffect(() => {
    setStatus(status);
  }, [status, setStatus]);

  useEffect(() => {
    if (totalSteps !== null) setTotalSteps(totalSteps);
  }, [totalSteps, setTotalSteps]);

  useEffect(() => {
    if (elapsedMs > 0) setElapsedMs(elapsedMs);
  }, [elapsedMs, setElapsedMs]);

  // Push new steps from WS into the store (only new ones)
  useEffect(() => {
    if (steps.length > storeSteps.length) {
      const newSteps = steps.slice(storeSteps.length);
      for (const s of newSteps) {
        pushStep(s);
      }
    }
  }, [steps, storeSteps.length, pushStep]);

  return (
    <div className="p-2 max-w-[1800px] mx-auto">
      {/* Run header */}
      <div className="flex items-center justify-between mb-2 px-1">
        <h1 className="text-[11px] font-bold tracking-wider">
          <span className="text-cyan">RUN</span>
          <span className="text-dim mx-1">/</span>
          <span className="text-purple">{runId.slice(0, 8)}</span>
          <span className="text-dim mx-1">/</span>
          <span className="text-text">LIVE</span>
        </h1>
        {error && (
          <span className="text-bearish text-[10px]">{error}</span>
        )}
      </div>

      {/* Main grid: 4 columns on xl, 2 on md, 1 on mobile */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-2">
        {/* Column 1: Left sidebar — sim meta + distributions */}
        <div className="space-y-2">
          <SimMeta />
          <PersonaDist />
          <TierDist />
          <StimulusWeights />
        </div>

        {/* Column 2-3: Center — price chart + social graph */}
        <div className="xl:col-span-2 space-y-2">
          <PriceChart />
          <SocialGraph runId={runId} recentActors={recentActors} />
        </div>

        {/* Column 4: Right sidebar — order book + feeds + god mode + forecast */}
        <div className="space-y-2">
          <OrderBook />
          <NewsFeed />
          <AgentFeed />
          <GodMode runId={runId} />
          <Forecast />
        </div>
      </div>

      {/* Bottom row: PnL + trades + eval mini */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-2 mt-2">
        <PnlHistory />
        <RecentTrades />
        <EvalMiniCard runId={runId} />
      </div>
    </div>
  );
}
