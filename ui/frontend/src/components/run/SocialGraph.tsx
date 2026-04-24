"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import {
  forceSimulation,
  forceLink,
  forceManyBody,
  forceCenter,
  forceCollide,
  type Simulation,
  type SimulationNodeDatum,
  type SimulationLinkDatum,
} from "d3-force";
import { zoom as d3Zoom, zoomIdentity, type ZoomBehavior } from "d3-zoom";
import { select } from "d3-selection";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

// Archetype -> semantic coloring via CSS variables (set in globals.css).
const ARCHETYPE_COLORS: Record<string, string> = {
  lurker: "var(--color-dim)",
  hodler: "var(--color-bullish)",
  paperhands: "var(--color-bearish)",
  fomo_degen: "var(--color-warn)",
  ta: "var(--color-cyan)",
  contrarian: "var(--color-purple)",
  news_trader: "var(--color-text)",
  whale: "var(--color-bullish)",
  kol: "var(--color-purple)",
  market_maker: "var(--color-dim)",
};

// Resolved hex colors for canvas 2D context (CSS vars don't work in canvas).
// Palette mirrors the DeSimulator/Defily tokens in lib/tokens.ts.
const ARCHETYPE_HEX: Record<string, string> = {
  lurker:       "#847e9c", // Defily muted lavender
  hodler:       "#00ff88",
  paperhands:   "#ff3355",
  fomo_degen:   "#ffaa00",
  ta:           "#7a2ff4", // Defily primary purple
  contrarian:   "#c84de8", // Defily magenta
  news_trader:  "#ffffff",
  whale:        "#00ff88",
  kol:          "#c84de8",
  market_maker: "#847e9c",
};

const EDGE_COLOR = "rgba(122, 47, 244, 0.18)";   // Defily purple tinted edges
const GLOW_COLOR = "rgba(122, 47, 244, 0.65)";   // pulse ring purple
const DEFAULT_NODE_COLOR = "#847e9c";

interface GraphNode extends SimulationNodeDatum {
  user_id: number;
  persona_id: string;
  archetype: string;
  name: string;
  follower_count: number;
  pulse: number; // 0..1, decays each frame
}

interface GraphLink extends SimulationLinkDatum<GraphNode> {
  source: number | GraphNode;
  target: number | GraphNode;
}

function nodeRadius(followerCount: number): number {
  return 2 + Math.log2(followerCount + 1) * 2;
}

function LegendItem({ color, label }: { color: string; label: string }) {
  return (
    <span className="flex items-center gap-1">
      <span
        className="inline-block w-2 h-2 rounded-full"
        style={{ backgroundColor: color }}
      />
      {label}
    </span>
  );
}

export function SocialGraph({
  runId,
  recentActors = [],
}: {
  runId: string;
  recentActors?: number[];
}) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [nodes, setNodes] = useState<GraphNode[]>([]);
  const [links, setLinks] = useState<GraphLink[]>([]);
  const simRef = useRef<Simulation<GraphNode, GraphLink> | null>(null);
  const transformRef = useRef({ k: 1, x: 0, y: 0 });
  const nodesRef = useRef<GraphNode[]>([]);
  const linksRef = useRef<GraphLink[]>([]);
  const rafRef = useRef<number>(0);
  const zoomRef = useRef<ZoomBehavior<HTMLCanvasElement, unknown> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Keep refs in sync with state for the render loop (avoids stale closures).
  useEffect(() => {
    nodesRef.current = nodes;
  }, [nodes]);
  useEffect(() => {
    linksRef.current = links;
  }, [links]);

  // ---- 1. Fetch graph data ----
  const fetchGraph = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/runs/${runId}/graph`);
      if (res.status === 404) {
        setError("waiting for simulation to initialize...");
        setLoading(false);
        // Retry in 3s
        retryTimerRef.current = setTimeout(fetchGraph, 3000);
        return;
      }
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const data = await res.json();
      const rawNodes: GraphNode[] = (
        data.nodes as Array<{
          user_id: number;
          persona_id: string;
          archetype: string;
          name: string;
          follower_count: number;
        }>
      ).map((n) => ({
        ...n,
        pulse: 0,
      }));

      const nodeMap = new Map(rawNodes.map((n) => [n.user_id, n]));
      const rawLinks: GraphLink[] = (
        data.edges as Array<{ source: number; target: number }>
      )
        .filter((e) => nodeMap.has(e.source) && nodeMap.has(e.target))
        .map((e) => ({ source: e.source, target: e.target }));

      setNodes(rawNodes);
      setLinks(rawLinks);
      setError(null);
      setLoading(false);

      const t0 = performance.now();
      console.debug(
        `[graph] rendering N=${rawNodes.length} in ${(performance.now() - t0).toFixed(1)}ms`
      );
    } catch (err) {
      setError(String(err));
      setLoading(false);
    }
  }, [runId]);

  useEffect(() => {
    fetchGraph();
    return () => {
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
    };
  }, [fetchGraph]);

  // ---- 2. Pulse logic: bump matching nodes when recentActors changes ----
  useEffect(() => {
    if (recentActors.length === 0) return;
    const actorSet = new Set(recentActors);
    setNodes((prev) =>
      prev.map((n) =>
        actorSet.has(n.user_id) ? { ...n, pulse: 1 } : n
      )
    );
  }, [recentActors]);

  // ---- 3. Force simulation ----
  useEffect(() => {
    if (nodes.length < 1) return;

    const canvas = canvasRef.current;
    const container = containerRef.current;
    if (!canvas) return;
    // Prefer the container's actual rendered size — canvas HTML attrs may be
    // stale (600x400) if ResizeObserver hasn't fired yet. This keeps nodes
    // centered regardless of timing.
    const rect = container?.getBoundingClientRect();
    const width = rect && rect.width > 0 ? rect.width : canvas.width;
    const height = rect && rect.height > 0 ? rect.height : canvas.height;

    // Build id lookup for forceLink
    const nodeById = new Map(nodes.map((n) => [n.user_id, n]));

    const sim = forceSimulation<GraphNode, GraphLink>(nodes)
      .force(
        "link",
        forceLink<GraphNode, GraphLink>(links)
          .id((d) => (d as GraphNode).user_id)
          .distance(50)
          .strength(0.5)
      )
      .force("charge", forceManyBody().strength(-30))
      .force("center", forceCenter(width / 2, height / 2))
      .force("collide", forceCollide<GraphNode>(10))
      .alphaDecay(0.01)
      .on("tick", () => {
        // no-op: drawing happens in RAF loop
      });

    // Run 200 warm-up ticks then set gentle settling
    sim.tick(200);
    sim.alphaTarget(0.01).restart();

    simRef.current = sim;

    return () => {
      sim.stop();
      simRef.current = null;
    };
    // Intentionally only re-run when nodes/links identity changes (initial load).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodes.length, links.length]);

  // ---- 4. Zoom/pan via d3-zoom ----
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const zoomBehavior = d3Zoom<HTMLCanvasElement, unknown>()
      .scaleExtent([0.3, 4])
      .on("zoom", (event) => {
        const t = event.transform;
        transformRef.current = { k: t.k, x: t.x, y: t.y };
      });

    select(canvas).call(zoomBehavior);
    zoomRef.current = zoomBehavior;

    return () => {
      select(canvas).on(".zoom", null);
    };
  }, []);

  // ---- 5. Canvas render loop (RAF) ----
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let running = true;

    function draw() {
      if (!running || !ctx || !canvas) return;

      const w = canvas.width;
      const h = canvas.height;
      const t = transformRef.current;
      const currentNodes = nodesRef.current;
      const currentLinks = linksRef.current;

      // Clear
      ctx.clearRect(0, 0, w, h);

      ctx.save();
      ctx.translate(t.x, t.y);
      ctx.scale(t.k, t.k);

      // Draw edges
      ctx.strokeStyle = EDGE_COLOR;
      ctx.lineWidth = 0.5;
      ctx.beginPath();
      for (const link of currentLinks) {
        const s = link.source as GraphNode;
        const tgt = link.target as GraphNode;
        if (
          s.x !== undefined &&
          s.y !== undefined &&
          tgt.x !== undefined &&
          tgt.y !== undefined
        ) {
          ctx.moveTo(s.x, s.y);
          ctx.lineTo(tgt.x, tgt.y);
        }
      }
      ctx.stroke();

      // Draw nodes
      for (const node of currentNodes) {
        if (node.x === undefined || node.y === undefined) continue;
        const r = nodeRadius(node.follower_count);
        const color = ARCHETYPE_HEX[node.archetype] ?? DEFAULT_NODE_COLOR;

        // Pulse glow ring
        if (node.pulse > 0.05) {
          const glowR = r + node.pulse * 8;
          ctx.beginPath();
          ctx.arc(node.x, node.y, glowR, 0, Math.PI * 2);
          ctx.fillStyle = `rgba(122, 47, 244, ${0.4 * node.pulse})`;
          ctx.fill();
          // Decay pulse
          node.pulse *= 0.95;
        } else if (node.pulse > 0) {
          node.pulse = 0;
        }

        // Node circle
        ctx.beginPath();
        ctx.arc(node.x, node.y, r, 0, Math.PI * 2);
        ctx.fillStyle = color;
        ctx.fill();
      }

      ctx.restore();

      rafRef.current = requestAnimationFrame(draw);
    }

    rafRef.current = requestAnimationFrame(draw);

    return () => {
      running = false;
      cancelAnimationFrame(rafRef.current);
    };
  }, []);

  // ---- 6. Resize canvas to container ----
  useEffect(() => {
    const container = containerRef.current;
    const canvas = canvasRef.current;
    if (!container || !canvas) return;

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        if (width > 0 && height > 0) {
          canvas.width = Math.round(width);
          canvas.height = Math.round(height);
        }
      }
    });

    observer.observe(container);
    // Set initial size
    const rect = container.getBoundingClientRect();
    if (rect.width > 0 && rect.height > 0) {
      canvas.width = Math.round(rect.width);
      canvas.height = Math.round(rect.height);
    }

    return () => observer.disconnect();
  }, []);

  // ---- Empty state ----
  const isEmpty = !loading && !error && nodes.length < 5;

  return (
    <div className="panel flex flex-col">
      <div className="panel-title">
        <span className="live-dot">SOCIAL GRAPH</span>
        <span className="ml-auto text-dim text-[10px]">
          {nodes.length} nodes &middot; {links.length} edges
        </span>
      </div>
      {loading && (
        <div className="text-dim text-center pt-10 text-[11px]">
          loading graph...
        </div>
      )}
      {error && (
        <div className="text-bearish text-center pt-10 text-[11px]">
          {error}
        </div>
      )}
      {isEmpty && (
        <div className="text-dim text-center pt-10 text-[11px]">
          no graph data
        </div>
      )}
      <div ref={containerRef} className="w-full h-[420px] relative">
        <canvas
          ref={canvasRef}
          width={600}
          height={420}
          className="absolute inset-0 w-full h-full"
        />
      </div>
      <div className="flex gap-3 text-[10px] text-dim mt-2 flex-wrap">
        <LegendItem color="var(--color-bullish)" label="Whale/HODLer" />
        <LegendItem color="var(--color-purple)" label="KOL/Contrarian" />
        <LegendItem color="var(--color-warn)" label="FOMO" />
        <LegendItem color="var(--color-bearish)" label="Paperhands" />
        <LegendItem color="var(--color-cyan)" label="TA" />
        <LegendItem color="var(--color-text)" label="News Trader" />
        <LegendItem color="var(--color-dim)" label="Lurker/MM" />
      </div>
    </div>
  );
}
