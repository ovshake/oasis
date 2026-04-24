/**
 * Shared types for the run telemetry views (live, replay, eval).
 * Keep in sync with backend WS shape and parquet schemas.
 */

// --- WebSocket step summary (from telemetry_ws.py _summarize_step) ---

export interface StepData {
  step: number;
  total_actions: number;
  tier_counts: Record<string, number>;
  archetype_counts: Record<string, number>;
  action_types: Record<string, number>;
}

// --- Price point for charts ---

export interface PricePoint {
  step: number;
  price: number;
}

// --- Order book level ---

export interface OrderLevel {
  price: number;
  size: number;
  total: number;
}

export interface OrderBookData {
  bids: OrderLevel[];
  asks: OrderLevel[];
  spread: number;
  midPrice: number;
}

// --- News event ---

export interface NewsEvent {
  step: number;
  title: string;
  content: string;
  source: string;
  sentiment: number;
  assets: string[];
}

// --- Agent action ---

export interface AgentAction {
  step: number;
  agent_name: string;
  archetype: string;
  action_type: string;
  details: string;
  follower_count?: number;
}

// --- Trade ---

export interface TradeRow {
  step: number;
  side: "buy" | "sell";
  price: number;
  qty: number;
  user_name: string;
}

// --- PnL point ---

export interface PnlPoint {
  step: number;
  wealth: number;
}

// --- Forecast ---

export interface ForecastData {
  target: number;
  stop: number;
  confidence: number;
  direction: "long" | "short";
}

// --- Eval report (from eval_report.json) ---

export interface MetricRow {
  name: string;
  value: number;
  threshold: number | null;
  passed: boolean | null;
  notes: string | null;
  direction?: string;
  ci_low?: number;
  ci_high?: number;
}

export interface EvalReport {
  generated_at: string;
  mode: string;
  run_dir: string;
  score_vector: Record<string, number>;
  metrics: MetricRow[];
  baselines: Record<string, MetricRow[]>;
  caveats: string[];
}

// --- Recent poster (simplified social graph MVP) ---

export interface RecentPoster {
  agent_name: string;
  archetype: string;
  follower_count: number;
  step: number;
}

// --- Social graph (from /api/runs/{id}/graph) ---

export interface RunGraphNode {
  user_id: number;
  persona_id: string;
  archetype: string;
  name: string;
  follower_count: number;
}

export interface RunGraphEdge {
  source: number;
  target: number;
}

export interface RunGraph {
  nodes: RunGraphNode[];
  edges: RunGraphEdge[];
}

// --- Run status ---

export type RunStatus = "connecting" | "running" | "completed" | "error";
