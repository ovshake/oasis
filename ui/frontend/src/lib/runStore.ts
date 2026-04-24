import { create } from "zustand";
import type {
  StepData,
  PricePoint,
  AgentAction,
  TradeRow,
  NewsEvent,
  OrderBookData,
  PnlPoint,
  ForecastData,
  RecentPoster,
  RunStatus,
} from "./types";

/**
 * Zustand store for aggregated run state across all live-view components.
 * WebSocket hook pushes step data here; components read from here.
 */

interface RunState {
  // Status
  status: RunStatus;
  scenarioName: string;
  seed: number;
  currentStep: number;
  totalSteps: number | null;
  elapsedMs: number;
  selectedAsset: string;
  assets: string[];

  // Aggregated telemetry
  steps: StepData[];
  prices: Record<string, PricePoint[]>;
  actions: AgentAction[];
  trades: TradeRow[];
  news: NewsEvent[];
  orderBook: OrderBookData;
  pnl: PnlPoint[];
  forecast: ForecastData | null;
  recentPosters: RecentPoster[];

  // Cumulative counts
  cumulativeTierCounts: Record<string, number>;
  cumulativeArchetypeCounts: Record<string, number>;

  // Actions
  setStatus: (s: RunStatus) => void;
  setMeta: (name: string, seed: number, assets: string[]) => void;
  setSelectedAsset: (asset: string) => void;
  pushStep: (step: StepData) => void;
  pushPrice: (asset: string, pt: PricePoint) => void;
  pushAction: (action: AgentAction) => void;
  pushTrade: (trade: TradeRow) => void;
  pushNews: (event: NewsEvent) => void;
  setOrderBook: (ob: OrderBookData) => void;
  pushPnl: (pt: PnlPoint) => void;
  setForecast: (f: ForecastData | null) => void;
  setTotalSteps: (n: number) => void;
  setElapsedMs: (ms: number) => void;

  // Bulk load (for replay)
  bulkLoad: (data: Partial<RunState>) => void;
  reset: () => void;
}

const INITIAL_ORDER_BOOK: OrderBookData = {
  bids: [],
  asks: [],
  spread: 0,
  midPrice: 0,
};

const MAX_ACTIONS = 200;
const MAX_TRADES = 100;
const MAX_NEWS = 100;

export const useRunStore = create<RunState>((set) => ({
  status: "connecting",
  scenarioName: "",
  seed: 0,
  currentStep: 0,
  totalSteps: null,
  elapsedMs: 0,
  selectedAsset: "BTC",
  assets: [],

  steps: [],
  prices: {},
  actions: [],
  trades: [],
  news: [],
  orderBook: INITIAL_ORDER_BOOK,
  pnl: [],
  forecast: null,
  recentPosters: [],

  cumulativeTierCounts: {},
  cumulativeArchetypeCounts: {},

  setStatus: (s) => set({ status: s }),
  setMeta: (name, seed, assets) =>
    set({ scenarioName: name, seed, assets, selectedAsset: assets[0] ?? "BTC" }),
  setSelectedAsset: (asset) => set({ selectedAsset: asset }),

  pushStep: (step) =>
    set((state) => {
      const newTierCounts = { ...state.cumulativeTierCounts };
      for (const [k, v] of Object.entries(step.tier_counts)) {
        newTierCounts[k] = (newTierCounts[k] ?? 0) + v;
      }
      const newArchCounts = { ...state.cumulativeArchetypeCounts };
      for (const [k, v] of Object.entries(step.archetype_counts)) {
        newArchCounts[k] = (newArchCounts[k] ?? 0) + v;
      }
      return {
        steps: [...state.steps, step],
        currentStep: step.step,
        cumulativeTierCounts: newTierCounts,
        cumulativeArchetypeCounts: newArchCounts,
      };
    }),

  pushPrice: (asset, pt) =>
    set((state) => ({
      prices: {
        ...state.prices,
        [asset]: [...(state.prices[asset] ?? []), pt],
      },
    })),

  pushAction: (action) =>
    set((state) => {
      const newActions = [...state.actions, action];
      if (newActions.length > MAX_ACTIONS) {
        newActions.splice(0, newActions.length - MAX_ACTIONS);
      }
      // Update recent posters (top 20 from last 10 steps)
      const cutoff = action.step - 10;
      const relevantActions = newActions.filter(
        (a) => a.step >= cutoff && a.action_type === "post",
      );
      const posterMap = new Map<string, RecentPoster>();
      for (const a of relevantActions) {
        if (
          !posterMap.has(a.agent_name) ||
          (posterMap.get(a.agent_name)?.step ?? 0) < a.step
        ) {
          posterMap.set(a.agent_name, {
            agent_name: a.agent_name,
            archetype: a.archetype,
            follower_count: a.follower_count ?? 0,
            step: a.step,
          });
        }
      }
      const recentPosters = Array.from(posterMap.values())
        .sort((a, b) => {
          if (a.archetype !== b.archetype) return a.archetype.localeCompare(b.archetype);
          return b.follower_count - a.follower_count;
        })
        .slice(0, 20);

      return { actions: newActions, recentPosters };
    }),

  pushTrade: (trade) =>
    set((state) => {
      const newTrades = [...state.trades, trade];
      if (newTrades.length > MAX_TRADES) {
        newTrades.splice(0, newTrades.length - MAX_TRADES);
      }
      return { trades: newTrades };
    }),

  pushNews: (event) =>
    set((state) => {
      const newNews = [...state.news, event];
      if (newNews.length > MAX_NEWS) {
        newNews.splice(0, newNews.length - MAX_NEWS);
      }
      return { news: newNews };
    }),

  setOrderBook: (ob) => set({ orderBook: ob }),
  pushPnl: (pt) => set((state) => ({ pnl: [...state.pnl, pt] })),
  setForecast: (f) => set({ forecast: f }),
  setTotalSteps: (n) => set({ totalSteps: n }),
  setElapsedMs: (ms) => set({ elapsedMs: ms }),

  bulkLoad: (data) => set((state) => ({ ...state, ...data })),

  reset: () =>
    set({
      status: "connecting",
      scenarioName: "",
      seed: 0,
      currentStep: 0,
      totalSteps: null,
      elapsedMs: 0,
      selectedAsset: "BTC",
      assets: [],
      steps: [],
      prices: {},
      actions: [],
      trades: [],
      news: [],
      orderBook: INITIAL_ORDER_BOOK,
      pnl: [],
      forecast: null,
      recentPosters: [],
      cumulativeTierCounts: {},
      cumulativeArchetypeCounts: {},
    }),
}));
