"use client";

import { useEffect, useState } from "react";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

interface ParquetResponse {
  run_id: string;
  section: string;
  count: number;
  rows: Record<string, unknown>[];
}

/**
 * Fetches a parquet section from the backend as JSON rows.
 * Used by the Replay view to load historical data (prices, trades, actions, etc.).
 */
export function useParquet(
  runId: string,
  section: string,
  enabled = true,
): { rows: Record<string, unknown>[]; loading: boolean; error: string | null } {
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!enabled || !runId || !section) return;

    let cancelled = false;
    setLoading(true);
    setError(null);

    fetch(`${API_BASE}/api/runs/${runId}/parquet/${section}`, {
      cache: "no-store",
    })
      .then((res) => {
        // Many parquet sections (news, trades, orders) are optional —
        // the writer skips them when there are zero rows to emit. A 404
        // here means "no data for this section," not a real error.
        if (res.status === 404) {
          if (!cancelled) {
            setRows([]);
            setLoading(false);
          }
          return null;
        }
        if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
        return res.json() as Promise<ParquetResponse>;
      })
      .then((data) => {
        if (!cancelled && data) {
          setRows(data.rows);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [runId, section, enabled]);

  return { rows, loading, error };
}
