"use client";

import { useRunStore } from "@/lib/runStore";

/** Scrollable list of news events. Sentiment bar: bull/bear/neutral coloring. */
export function NewsFeed() {
  const news = useRunStore((s) => s.news);

  return (
    <div className="panel">
      <h3 className="panel-title">News Feed</h3>
      {news.length === 0 ? (
        <p className="text-dim text-[10px]">No news events yet...</p>
      ) : (
        <div className="space-y-1 max-h-48 overflow-y-auto">
          {news.map((ev, i) => {
            const sentColor =
              ev.sentiment > 0.2
                ? "bg-bullish/30"
                : ev.sentiment < -0.2
                  ? "bg-bearish/30"
                  : "bg-dim/30";
            const sentLabel =
              ev.sentiment > 0.2
                ? "BULL"
                : ev.sentiment < -0.2
                  ? "BEAR"
                  : "NEUT";
            return (
              <div
                key={`${ev.step}-${i}`}
                className="flex items-start gap-2 text-[10px] py-0.5 border-b border-border/50 last:border-0"
              >
                <span className="text-dim tabular-nums shrink-0">
                  s{ev.step}
                </span>
                <span className="px-1 py-0.5 bg-cyan/10 text-cyan text-[9px] font-bold shrink-0">
                  {ev.source.slice(0, 6).toUpperCase()}
                </span>
                <span
                  className={`px-1 py-0.5 text-[9px] font-bold shrink-0 ${sentColor}`}
                >
                  {sentLabel}
                </span>
                <span
                  className="text-text truncate flex-1"
                  title={ev.content}
                >
                  {ev.title || ev.content}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
