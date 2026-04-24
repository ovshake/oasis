"use client";

import { useEffect, useState } from "react";

function utcClock(): string {
  const d = new Date();
  return d.toISOString().replace("T", " ").slice(0, 19) + " UTC";
}

export function TopBar() {
  const [time, setTime] = useState<string>("");

  useEffect(() => {
    setTime(utcClock());
    const id = setInterval(() => setTime(utcClock()), 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <header className="flex items-center justify-between px-4 py-2 border-b border-border-bright bg-panel">
      {/* Left: branding */}
      <div className="flex items-center gap-2">
        <span
          className="text-cyan font-bold text-sm tracking-wider"
          style={{ textShadow: "0 0 12px rgba(0,221,255,0.3)" }}
        >
          OASIS
        </span>
        <span className="text-dim text-[10px] uppercase tracking-widest">
          Crypto Narrative Simulator
        </span>
      </div>

      {/* Center: price ticker placeholder */}
      <div className="hidden md:flex items-center gap-4 text-[11px]">
        <span className="text-dim">
          PRICES — connect via live run
        </span>
      </div>

      {/* Right: UTC clock + status */}
      <div className="flex items-center gap-3 text-[11px]">
        <span className="text-dim font-mono tabular-nums">{time}</span>
        <span className="live-dot text-[10px] text-cyan">READY</span>
      </div>
    </header>
  );
}
