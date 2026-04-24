"use client";

import { useEffect, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

interface Post {
  post_id: number;
  user_id: number;
  content: string;
  created_at: string;
  likes: number;
  dislikes: number;
  handle: string | null;
  persona_id: string | null;
  archetype: string;
}

interface Comment {
  comment_id: number;
  post_id: number;
  user_id: number;
  content: string;
  created_at: string;
  handle: string | null;
  archetype: string;
}

interface FeedResponse {
  posts: Post[];
  comments: Comment[];
  counts: { posts: number; comments: number };
}

// Semantic coloring per archetype. Chrome = cyan/purple for handle + time.
const ARCHETYPE_TEXT: Record<string, string> = {
  kol: "text-purple",
  whale: "text-bullish",
  hodler: "text-bullish",
  fomo_degen: "text-warn",
  paperhands: "text-bearish",
  ta: "text-cyan",
  contrarian: "text-purple",
  news_trader: "text-text",
  market_maker: "text-dim",
  lurker: "text-dim",
};

function archetypeColor(a: string): string {
  return ARCHETYPE_TEXT[a] ?? "text-text";
}

/**
 * Social Feed panel — renders agent-authored posts (and optionally their
 * comment thread) from the run's simulation.db. Polls every 3s so the feed
 * stays fresh during live runs; for completed runs the poll just re-reads
 * the same final state.
 */
export function SocialFeed({
  runId,
  showComments = false,
  maxHeight = "max-h-96",
}: {
  runId: string;
  showComments?: boolean;
  maxHeight?: string;
}) {
  const [data, setData] = useState<FeedResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!runId) return;
    let cancelled = false;

    async function poll() {
      try {
        const res = await fetch(
          `${API_BASE}/api/runs/${runId}/posts?limit=200&include_comments=${showComments}`,
        );
        if (res.status === 404) {
          if (!cancelled) setError("waiting for simulation to initialize...");
          return;
        }
        if (!res.ok) {
          if (!cancelled) setError(`HTTP ${res.status}`);
          return;
        }
        const json = (await res.json()) as FeedResponse;
        if (!cancelled) {
          setData(json);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) setError(String(err));
      }
    }

    poll();
    const interval = setInterval(poll, 3000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [runId, showComments]);

  const posts = data?.posts ?? [];
  const comments = data?.comments ?? [];
  const totalPosts = data?.counts.posts ?? 0;
  const totalComments = data?.counts.comments ?? 0;

  // Build comment lookup: post_id -> comment[]
  const commentsByPost = new Map<number, Comment[]>();
  for (const c of comments) {
    const arr = commentsByPost.get(c.post_id) ?? [];
    arr.push(c);
    commentsByPost.set(c.post_id, arr);
  }

  return (
    <div className="panel">
      <div className="panel-title">
        <span className="live-dot">SOCIAL FEED</span>
        <span className="ml-auto text-dim text-[10px] tabular-nums">
          {totalPosts.toLocaleString()} posts
          {showComments && ` · ${totalComments.toLocaleString()} comments`}
        </span>
      </div>

      {error && (
        <p className="text-bearish text-[10px]">{error}</p>
      )}

      {!error && posts.length === 0 && (
        <p className="text-dim text-[10px]">No posts yet...</p>
      )}

      {posts.length > 0 && (
        <div className={`${maxHeight} overflow-y-auto space-y-1 pr-1`}>
          {posts.map((p) => (
            <div
              key={p.post_id}
              className="text-[11px] border-b border-border/40 pb-1 last:border-0"
            >
              <div className="flex items-baseline gap-2">
                <span className={`${archetypeColor(p.archetype)} font-bold truncate max-w-[140px]`} title={p.handle ?? ""}>
                  @{p.handle ?? `user${p.user_id}`}
                </span>
                <span className="text-dim text-[9px] uppercase tracking-wider shrink-0">
                  {p.archetype.replace("_", " ")}
                </span>
                <span className="ml-auto text-dim text-[9px] tabular-nums shrink-0">
                  #{p.post_id}
                </span>
              </div>
              <div className="text-text mt-0.5 break-words">{p.content}</div>
              {showComments &&
                (commentsByPost.get(p.post_id) ?? [])
                  .slice(0, 5)
                  .map((c) => (
                    <div
                      key={c.comment_id}
                      className="mt-0.5 ml-3 pl-2 border-l border-border text-[10px]"
                    >
                      <span className={`${archetypeColor(c.archetype)} font-bold`}>
                        @{c.handle ?? `user${c.user_id}`}
                      </span>
                      <span className="text-dim ml-1">{c.content}</span>
                    </div>
                  ))}
              {p.likes + p.dislikes > 0 && (
                <div className="mt-0.5 text-[9px] text-dim">
                  ♥ {p.likes}  {p.dislikes > 0 && `· ↓ ${p.dislikes}`}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
