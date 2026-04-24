"use client";

import { useState, useCallback } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

const AUDIENCES = ["all", "news_traders", "kols", "crypto_natives", "whales"] as const;
type Audience = (typeof AUDIENCES)[number];

const MAGNITUDES = ["minor", "moderate", "major", "critical"] as const;
type Magnitude = (typeof MAGNITUDES)[number];

const CREDIBILITIES = ["rumor", "reported", "confirmed"] as const;
type Credibility = (typeof CREDIBILITIES)[number];

const ASSETS = ["BTC", "ETH", "USDT", "XAU", "WTI"] as const;

type Status = "idle" | "sending" | "ok" | "error";

interface GodModeProps {
  runId: string;
}

export function GodMode({ runId }: GodModeProps) {
  const [title, setTitle] = useState("");
  const [content, setContent] = useState("");
  const [sentiment, setSentiment] = useState(0);
  const [assets, setAssets] = useState<string[]>([]);
  const [audience, setAudience] = useState<Audience>("all");
  const [magnitude, setMagnitude] = useState<Magnitude>("moderate");
  const [credibility, setCredibility] = useState<Credibility>("reported");
  const [status, setStatus] = useState<Status>("idle");
  const [lastEvent, setLastEvent] = useState<{ step: number; title: string } | null>(null);
  const [errorMsg, setErrorMsg] = useState("");

  const toggleAsset = useCallback((asset: string) => {
    setAssets((prev) =>
      prev.includes(asset) ? prev.filter((a) => a !== asset) : [...prev, asset]
    );
  }, []);

  const canSubmit = title.trim().length > 0 && content.trim().length > 0 && status !== "sending";

  const inject = useCallback(async () => {
    if (!canSubmit) return;
    setStatus("sending");
    setErrorMsg("");
    try {
      const res = await fetch(`${API_BASE}/api/runs/${runId}/inject-news`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title,
          content,
          sentiment_valence: sentiment,
          affected_assets: assets,
          audience,
          magnitude,
          credibility,
        }),
      });
      if (!res.ok) {
        const detail = await res.text().catch(() => res.statusText);
        throw new Error(detail);
      }
      const data = await res.json();
      setLastEvent({ step: data.step, title });
      setStatus("ok");
      setTimeout(() => {
        setTitle("");
        setContent("");
        setSentiment(0);
        setAssets([]);
        setAudience("all");
        setMagnitude("moderate");
        setCredibility("reported");
        setStatus("idle");
      }, 2000);
    } catch (err) {
      setStatus("error");
      setErrorMsg(err instanceof Error ? err.message : "Unknown error");
    }
  }, [canSubmit, title, content, sentiment, assets, audience, magnitude, credibility, runId]);

  const sentimentLabel =
    sentiment > 0.1 ? "BULL" : sentiment < -0.1 ? "BEAR" : "NEUT";
  const sentimentColorClass =
    sentiment > 0.1 ? "text-bullish" : sentiment < -0.1 ? "text-bearish" : "text-dim";

  return (
    <div className="panel">
      <div className="panel-title flex items-center justify-between">
        <span>GOD MODE &middot; NEWS INJECTOR</span>
        {status === "ok" && lastEvent && (
          <span className="text-bullish text-[9px] ml-auto">
            fires at step {lastEvent.step}
          </span>
        )}
        {status === "error" && (
          <span className="text-bearish text-[9px] ml-auto" title={errorMsg}>
            error
          </span>
        )}
        {status === "sending" && (
          <span className="text-cyan text-[9px] ml-auto animate-pulse">
            sending...
          </span>
        )}
      </div>

      <div className="space-y-2 mt-1">
        {/* Title */}
        <input
          type="text"
          placeholder="Headline title..."
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          className="w-full bg-panelAlt text-text text-[10px] font-mono
                     border border-border rounded px-2 py-1
                     placeholder:text-dim/50
                     focus:border-cyan focus:outline-none"
        />

        {/* Content */}
        <textarea
          placeholder="News body / details..."
          value={content}
          onChange={(e) => setContent(e.target.value)}
          rows={3}
          className="w-full bg-panelAlt text-text text-[10px] font-mono
                     border border-border rounded px-2 py-1 resize-none
                     placeholder:text-dim/50
                     focus:border-cyan focus:outline-none"
        />

        {/* Sentiment slider */}
        <div className="flex items-center gap-2">
          <span className="text-[9px] text-dim shrink-0 w-16">SENTIMENT</span>
          <input
            type="range"
            min={-1}
            max={1}
            step={0.1}
            value={sentiment}
            onChange={(e) => setSentiment(parseFloat(e.target.value))}
            className="flex-1 h-1 accent-cyan"
          />
          <span className={`text-[10px] font-bold tabular-nums w-16 text-right ${sentimentColorClass}`}>
            {sentiment > 0 ? "+" : ""}
            {sentiment.toFixed(1)} {sentimentLabel}
          </span>
        </div>

        {/* Asset toggles */}
        <div className="flex items-center gap-1">
          <span className="text-[9px] text-dim shrink-0 w-16">ASSETS</span>
          <div className="flex flex-wrap gap-1">
            {ASSETS.map((a) => {
              const active = assets.includes(a);
              return (
                <button
                  key={a}
                  type="button"
                  onClick={() => toggleAsset(a)}
                  className={`px-1.5 py-0.5 text-[9px] font-bold font-mono border rounded
                    transition-colors
                    ${
                      active
                        ? "bg-cyan/20 text-cyan border-cyan/50"
                        : "bg-transparent text-dim border-border hover:border-dim"
                    }`}
                >
                  {a}
                </button>
              );
            })}
          </div>
        </div>

        {/* Audience + Magnitude + Credibility dropdowns */}
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1 flex-1">
            <span className="text-[9px] text-dim shrink-0">AUD</span>
            <select
              value={audience}
              onChange={(e) => setAudience(e.target.value as Audience)}
              className="flex-1 bg-panelAlt text-text text-[9px] font-mono
                         border border-border rounded px-1 py-0.5
                         focus:border-cyan focus:outline-none"
            >
              {AUDIENCES.map((a) => (
                <option key={a} value={a}>
                  {a}
                </option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-1 flex-1">
            <span className="text-[9px] text-dim shrink-0">MAG</span>
            <select
              value={magnitude}
              onChange={(e) => setMagnitude(e.target.value as Magnitude)}
              className="flex-1 bg-panelAlt text-text text-[9px] font-mono
                         border border-border rounded px-1 py-0.5
                         focus:border-cyan focus:outline-none"
            >
              {MAGNITUDES.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-1 flex-1">
            <span className="text-[9px] text-dim shrink-0">CRED</span>
            <select
              value={credibility}
              onChange={(e) => setCredibility(e.target.value as Credibility)}
              className="flex-1 bg-panelAlt text-text text-[9px] font-mono
                         border border-border rounded px-1 py-0.5
                         focus:border-cyan focus:outline-none"
            >
              {CREDIBILITIES.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Inject button */}
        <button
          type="button"
          disabled={!canSubmit}
          onClick={inject}
          className={`w-full py-1.5 text-[10px] font-bold font-mono uppercase
                      tracking-widest border rounded transition-all
                      ${
                        canSubmit
                          ? "bg-cyan/10 text-cyan border-cyan/50 hover:bg-cyan/20 hover:shadow-[0_0_12px_rgba(0,221,255,0.3)]"
                          : "bg-transparent text-dim/40 border-border/40 cursor-not-allowed"
                      }`}
        >
          {status === "sending" ? "Injecting..." : "Inject News"}
        </button>
      </div>
    </div>
  );
}
