import Link from "next/link";
import { api, type ScenarioSummary, type RunInfo } from "@/lib/api";

// ---------------------------------------------------------------------------
// Data fetchers (server-side, no cache for local API)
// ---------------------------------------------------------------------------

async function fetchScenarios(): Promise<ScenarioSummary[]> {
  try {
    return await api.listScenarios();
  } catch {
    return [];
  }
}

async function fetchRuns(): Promise<RunInfo[]> {
  try {
    return await api.listRuns();
  } catch {
    return [];
  }
}

// ---------------------------------------------------------------------------
// Status chip coloring (semantic only)
// ---------------------------------------------------------------------------

function statusClass(status: string): string {
  switch (status) {
    case "completed":
      return "text-bullish";
    case "running":
      return "text-cyan";
    case "failed":
      return "text-bearish";
    case "stopped":
      return "text-warn";
    default:
      return "text-dim";
  }
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export default async function HomePage() {
  const [scenarios, runs] = await Promise.all([
    fetchScenarios(),
    fetchRuns(),
  ]);

  return (
    <div className="p-4 max-w-7xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-bold tracking-wider flex items-baseline gap-2">
          <span className="brand-glow text-cyan">DESIMULATOR</span>
          <span className="text-dim mx-1">/</span>
          <span className="text-text">NARRATIVE MARKET SIM</span>
          <span className="text-dim text-[9px] uppercase tracking-widest ml-2">
            by{" "}
            <a
              href="https://www.defily.ai/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-dim hover:text-cyan transition-colors"
            >
              Defily
            </a>
          </span>
        </h1>
        <Link
          href="/scenarios/new"
          className="px-3 py-1.5 text-[11px] font-bold uppercase tracking-wider
                     bg-cyan/10 text-cyan border border-cyan/30
                     hover:bg-cyan/20 transition-colors"
        >
          + New Scenario
        </Link>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Recent Runs */}
        <section className="panel">
          <h2 className="panel-title">
            <span className="live-dot">Recent Runs</span>
          </h2>
          {runs.length === 0 ? (
            <p className="text-dim text-[11px] py-4">
              No runs yet. Create a scenario and start a simulation.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-cyan text-left text-[10px] uppercase tracking-widest">
                    <th className="py-1 pr-3">Run ID</th>
                    <th className="py-1 pr-3">Scenario</th>
                    <th className="py-1 pr-3">Status</th>
                    <th className="py-1 pr-3">Started</th>
                    <th className="py-1 pr-3">Links</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <tr
                      key={run.run_id}
                      className="border-t border-border hover:bg-panel-alt transition-colors"
                    >
                      <td className="py-1.5 pr-3 text-purple">
                        <Link
                          href={
                            run.status === "running"
                              ? `/run/${run.run_id}`
                              : `/run/${run.run_id}/replay`
                          }
                          className="hover:underline"
                        >
                          {run.run_id.slice(0, 8)}
                        </Link>
                      </td>
                      <td className="py-1.5 pr-3">{run.scenario_name}</td>
                      <td className={`py-1.5 pr-3 font-bold uppercase ${statusClass(run.status)}`}>
                        {run.status}
                      </td>
                      <td className="py-1.5 pr-3 text-dim">
                        {run.started_at ?? "--"}
                      </td>
                      <td className="py-1.5 pr-3 text-[10px] space-x-2">
                        {run.status === "running" && (
                          <Link
                            href={`/run/${run.run_id}`}
                            className="text-cyan hover:underline"
                          >
                            LIVE
                          </Link>
                        )}
                        {run.status === "completed" && (
                          <>
                            <Link
                              href={`/run/${run.run_id}/replay`}
                              className="text-warn hover:underline"
                            >
                              REPLAY
                            </Link>
                            <Link
                              href={`/eval/${run.run_id}`}
                              className="text-purple hover:underline"
                            >
                              EVAL
                            </Link>
                          </>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        {/* Scenario Library */}
        <section className="panel">
          <h2 className="panel-title">Scenario Library</h2>
          {scenarios.length === 0 ? (
            <p className="text-dim text-[11px] py-4">
              No scenarios found. Create one to get started.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="text-cyan text-left text-[10px] uppercase tracking-widest">
                    <th className="py-1 pr-3">Name</th>
                    <th className="py-1 pr-3">Steps</th>
                    <th className="py-1 pr-3">Agents</th>
                    <th className="py-1 pr-3">Source</th>
                  </tr>
                </thead>
                <tbody>
                  {scenarios.map((s) => (
                    <tr
                      key={`${s.source_dir}/${s.name}`}
                      className="border-t border-border hover:bg-panel-alt transition-colors"
                    >
                      <td className="py-1.5 pr-3">
                        <Link
                          href={`/scenarios/${s.name}`}
                          className="text-text hover:text-cyan hover:underline transition-colors"
                        >
                          {s.name}
                        </Link>
                      </td>
                      <td className="py-1.5 pr-3 text-dim">
                        {s.duration_steps ?? "--"}
                      </td>
                      <td className="py-1.5 pr-3 text-dim">
                        {s.agents_count ?? "--"}
                      </td>
                      <td className="py-1.5 pr-3 text-dim text-[10px]">
                        {s.source_dir}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
