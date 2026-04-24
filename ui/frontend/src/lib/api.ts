/**
 * Typed REST client for the OASIS Crypto Sim backend (FastAPI on :8000).
 * All fetch calls return parsed JSON. Errors propagate as rejected promises.
 */

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

// ---------------------------------------------------------------------------
// Types mirroring backend schemas
// ---------------------------------------------------------------------------

export interface ScenarioSummary {
  name: string;
  path: string;
  duration_steps: number | null;
  agents_count: number | null;
  llm_enabled: boolean;
  source_dir: string;
}

export interface ManualNewsEvent {
  step: number;
  content: string;
  title?: string;
  sentiment: number;
  assets: string[];
  audience: string;
}

export interface PopulationMix {
  lurker: number;
  hodler: number;
  paperhands: number;
  fomo_degen: number;
  ta: number;
  contrarian: number;
  news_trader: number;
  whale: number;
  kol: number;
  market_maker: number;
}

export interface NewsSourceSpec {
  kind: "manual" | "historical" | "live_snapshot";
  providers: string[];
  date_range: string[] | null;
  lookback_hours: number | null;
  relevance_filter: Record<string, unknown>;
  enrich_with: string;
}

export interface Scenario {
  name: string;
  duration_steps: number;
  step_minutes: number;
  seed: number;
  agents_count: number;
  assets: string[];
  price_source: "default" | "live" | "historical" | "manual";
  as_of_date: string | null;
  initial_prices: Record<string, number>;
  population_mix: PopulationMix;
  news_source: NewsSourceSpec;
  manual_events: ManualNewsEvent[];
  persona_library: string;
  llm_enabled: boolean;
  output_dir: string;
}

export interface RunInfo {
  run_id: string;
  scenario_name: string;
  status: "running" | "completed" | "failed" | "stopped";
  seed: number;
  pid: number | null;
  output_dir: string;
  started_at: string;
  finished_at: string | null;
}

export interface PersonaSummary {
  persona_id: string;
  archetype: string;
  name: string;
  backstory?: string;
  voice_style?: string;
}

export interface PersonaDistribution {
  total: number;
  distribution: Record<string, number>;
}

// ---------------------------------------------------------------------------
// API functions
// ---------------------------------------------------------------------------

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    const detail = await res.text().catch(() => res.statusText);
    throw new Error(`GET ${path} failed (${res.status}): ${detail}`);
  }
  return res.json() as Promise<T>;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => res.statusText);
    throw new Error(`POST ${path} failed (${res.status}): ${detail}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  // Scenarios
  listScenarios: () => get<ScenarioSummary[]>("/api/scenarios"),
  getScenario: (name: string) => get<Scenario>(`/api/scenarios/${name}`),
  saveScenario: (s: Record<string, unknown>) =>
    post<{ name: string; path: string; created: boolean }>("/api/scenarios", s),

  // Runs
  listRuns: () => get<RunInfo[]>("/api/runs"),
  getRun: (id: string) => get<RunInfo>(`/api/runs/${id}`),
  startRun: (scenario_name: string, seeds: number[] = [42], no_llm = false) =>
    post<{ run_id: string; pid: number; output_dir: string }>("/api/runs", {
      scenario_name,
      seeds,
      no_llm,
    }),
  getRunGraph: (runId: string) =>
    get<{ nodes: Array<{ user_id: number; persona_id: string; archetype: string; name: string; follower_count: number }>; edges: Array<{ source: number; target: number }> }>(
      `/api/runs/${runId}/graph`
    ),

  // Personas
  listPersonas: (archetype?: string, limit = 50) =>
    get<{ total: number; offset: number; limit: number; personas: PersonaSummary[] }>(
      `/api/personas?${archetype ? `archetype=${archetype}&` : ""}limit=${limit}`
    ),
  personaDistribution: () => get<PersonaDistribution>("/api/personas/distribution"),
};
