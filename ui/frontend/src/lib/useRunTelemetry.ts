"use client";

import { useEffect, useRef, useState } from "react";
import type { StepData, RunStatus } from "./types";

/**
 * WebSocket hook that connects to /ws/runs/{runId} and accumulates
 * step-by-step telemetry data.
 *
 * Returns steps[], status, and any error string. Cleans up on unmount.
 */
export function useRunTelemetry(runId: string) {
  const [steps, setSteps] = useState<StepData[]>([]);
  const [status, setStatus] = useState<RunStatus>("connecting");
  const [error, setError] = useState<string | null>(null);
  const [totalSteps, setTotalSteps] = useState<number | null>(null);
  const [elapsedMs, setElapsedMs] = useState<number>(0);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    // Defer the WebSocket connect until caller confirms they want it by
    // passing a non-empty runId. Lets parent pages gate the connect on
    // e.g. "only connect if run is actually running".
    if (!runId) return;
    const API_BASE =
      process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";
    const wsUrl = API_BASE.replace(/^http/, "ws") + `/ws/runs/${runId}`;
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => setStatus("running");

    ws.onmessage = (ev: MessageEvent) => {
      try {
        const msg = JSON.parse(ev.data as string) as Record<string, unknown>;
        if (msg.type === "step") {
          const data = msg.data as Record<string, unknown>;
          setSteps((prev) => [
            ...prev,
            {
              step: msg.step as number,
              total_actions: (data.total_actions as number) ?? 0,
              tier_counts: (data.tier_counts as Record<string, number>) ?? {},
              archetype_counts:
                (data.archetype_counts as Record<string, number>) ?? {},
              action_types:
                (data.action_types as Record<string, number>) ?? {},
            },
          ]);
        } else if (msg.type === "complete") {
          setStatus("completed");
          setTotalSteps((msg.total_steps as number) ?? null);
          setElapsedMs((msg.elapsed_ms as number) ?? 0);
        } else if (msg.type === "error") {
          setStatus("error");
          setError((msg.message as string) ?? "Unknown error");
        }
      } catch {
        // Ignore malformed messages
      }
    };

    ws.onerror = () => {
      setStatus("error");
      setError("WebSocket connection failed");
    };

    ws.onclose = () => {
      if (status === "connecting") {
        setStatus("error");
        setError("WebSocket closed before connecting");
      }
    };

    return () => {
      ws.close();
    };
    // Only re-run when runId changes
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId]);

  return { steps, status, error, totalSteps, elapsedMs };
}
