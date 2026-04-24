# DeSimulator

**A "what if?" engine for crypto markets.**

Ask questions like:
- *"What happens to BTC if the Fed cuts rates next week?"*
- *"If a big KOL tweets about ETH, how fast does the news spread and what
  does the chart look like?"*
- *"What does a Luna-style stablecoin depeg do to the rest of the market?"*

DeSimulator runs the scenario with hundreds or thousands of AI-driven
trader-agents — each with its own persona — and shows you the resulting
price action, order flow, social feed, and P&L in a Bloomberg-style
terminal.

Built by [Defily](https://www.defily.ai/).

---

## What makes it different from a backtest

A backtest replays what *did* happen. DeSimulator runs what *might* happen.

- **Markets move because people act.** We simulate the people directly
  — HODLers hold, FOMO degens chase pumps, paperhands panic-sell,
  whales accumulate, KOLs post threads, market makers quote spreads.
- **News drives narrative.** You can hand-inject a headline at any
  point in the timeline — *"Fed pauses hikes"* at step 50, *"SEC
  approves ETF"* at step 120 — and watch the reaction propagate
  through the social graph into prices.
- **Every run is explorable.** You get a replay with step-by-step
  scrubber: rewind, inspect any moment, see which agent tweeted
  what, watch the order book evolve.

---

## Opening the app

If your environment is already set up, just open **http://localhost:3000**
in your browser.

If you're starting from scratch, ask whoever set this up to run the two
start commands for you. Starting the servers is a one-time technical
step; once they're running, everything below is point-and-click.

---

## The home page

![Home page](docs/screenshots/01-home.png)

Two tables.

**Recent Runs** (left) — every simulation you've run. Each row has a
REPLAY link (opens the analysis view) and an EVAL link (opens the scorecard).

**Scenario Library** (right) — ready-made scenarios you can run with one
click, or use as templates. The defaults:

| Name | What it tests |
|---|---|
| `quiet_market` | Baseline — no news, just organic trading. Useful as a control. |
| `fed_hawkish` | A single bearish macro announcement. How do prices react to a surprise rate hike? |
| `kol_pump` | A KOL announces a $10M BTC buy. Does it pump? How fast does it spread? |
| `live_today` | Real live prices + real news from the last 24 hours. "What would today look like?" |
| `llm_quick_demo` | 40 agents, 30 steps, fast — best for seeing the system come alive. |
| `luna_depeg` (calibration) | Replays the UST stablecoin depeg of May 2022. Used to validate the sim against real history. |

Click **+ New Scenario** (top right) to build your own.

---

## Building a scenario

![Scenario builder](docs/screenshots/02-scenario-builder.png)

### The basics

- **Name** — whatever you want to call it.
- **Duration (steps)** — how long the sim runs. Each step is 1 minute of
  simulated market time. 240 steps = a 4-hour trading window.
- **Seed** — change this to get a different random path through the same
  scenario. Same seed = same results every time.
- **Agents** — how many trader-agents participate. 100 is fast and
  readable; 1,000 gives more realistic order-book depth; 10,000 for
  calibration work.

### The LLM toggle (important)

**OFF (default):** Agents act on heuristic rules. Fast — a 240-step run
finishes in about 30 seconds. **Free.** Good for quick iteration and
testing scenario setup.

**ON:** Agents use Claude to *actually think* about news, their
personality, and market state. Posts are written in their own voice,
trades come from reasoning. A live cost and time estimate appears next
to the toggle as you change duration and agent count.

Rule of thumb for demos: keep it **OFF**. Turn it **ON** when you
specifically want realistic narrative — agent posts, social propagation,
behaviorally-driven price moves.

### Assets

Toggle which of **BTC, ETH, USDT, XAU (gold), WTI (oil), USD** are
active. USD is always the quote currency. USDT is pegged unless you
manually override it (useful for depeg scenarios).

### Price source

- **Default** — nice round seeded prices (BTC $80k, ETH $3.5k, etc.)
- **Live** — pulls the current market price when the run starts
- **Historical** — pulls prices from a specific past date (e.g., start
  the sim at Luna-depeg-day prices)
- **Manual** — type in whatever starting prices you want

### News source

![News editor](docs/screenshots/03-news-editor.png)

Three modes, each explained inline on the card:

- **MANUAL** — you write the headlines yourself on a timeline.
- **HISTORICAL** — fetches real news from CryptoPanic for a past date range.
- **LIVE SNAPSHOT** — fetches real news from the last N hours.

Each timeline event has:

- **Step** — when the news breaks (step 50 = 50 minutes into the sim).
- **Audience** — who sees it first. `all` means broadcast; `kols` means
  it leaks to the influencers first and propagates from there. This is
  the key knob for studying narrative diffusion.
- **Headline / content** — the text agents read.
- **Sentiment** — a slider from −1 (max bearish) to +1 (max bullish).
  The slider is color-coded BULL/NEUT/BEAR so you don't have to guess.
- **Affected assets** — toggle chips for BTC / ETH / USDT / XAU / WTI.
  Agents holding these react more strongly.

You can mix modes: e.g. start with real news from yesterday and layer
one hypothetical "what if the Fed pauses at step 100" event on top.

### Population mix

Ten sliders — the archetypes that make up your sim population. They
must sum to 100%.

| Archetype | What they do |
|---|---|
| **Lurker** | Watches, rarely acts. Most of the crowd. |
| **HODLer** | Long conviction, holds through drawdowns. |
| **Paperhands** | Panic-sells on bad news. |
| **FOMO Degen** | Chases pumps, buys the top. |
| **TA** | Trades off technical signals. |
| **Contrarian** | Buys fear, sells greed. |
| **News Trader** | Reacts fast to headlines. |
| **Whale** | Big capital, slow-moving, patient. |
| **KOL** | Influencer — posts a lot, has followers. |
| **Market Maker** | Quotes both sides, provides liquidity. |

The default mix (45% Lurker, 15% HODLer/Paperhands, small whale/KOL
tails) matches the real-world social-media rule-of-thumb that 90% lurk,
9% engage, 1% drive. Tweak to test edge cases — *"what if half the
population were FOMO degens?"* is one slider away.

### Running it

- **SAVE** — stores the scenario so you can come back later.
- **RUN** — saves and launches immediately. You're taken to the live
  view while the run progresses.

---

## Watching a run

![Replay view](docs/screenshots/04-replay-view.png)

This is the main analysis view. Lots of panels — here's what each one
tells you:

### Top bar

Five-asset ticker with today's % change, live UTC clock, and the
DeSimulator wordmark linking to Defily. Think of it as your status line.

### Playback controls (just under the header)

- **PLAY / PAUSE** — step through the simulation
- **1x / 10x / 100x** — playback speed
- **Scrubber** — click or drag anywhere on the timeline; all panels
  snap to that moment

### Left rail — "who's in the sim and what's pushing them"

- **Simulation** — the scenario name, seed, current step, status
- **Persona Distribution** — how many of each archetype got sampled
  for this run
- **Tier Distribution** — at the current moment, what fraction of
  agents are silent vs reacting vs commenting vs posting vs trading.
  ~87% silent in a quiet market is *normal* — real social media
  follows the 1-9-90 rule.
- **Stimulus Weights** — what's driving agents to act right now.
  If "News" is at 80%, a recent event is the dominant signal; if
  "Price" is high, agents are reacting to portfolio moves. Useful
  for asking "why did the market just spike?"

### Center — "what's actually happening in the market"

- **Price Chart** — one line per asset. Tabs at the top to switch
  between BTC / ETH / USDT / XAU / WTI.
- **Social Graph** — a force-directed "web" of agents, colored by
  archetype. When an agent posts, their node pulses. When info
  propagates along follow-edges, you see the cascade ripple through
  the graph in real time. This is the payoff visualization — no
  other crypto sim shows this.

### Right rail — "live market microstructure"

- **Order Book** — L2 bids (green) and asks (red) for the selected
  pair. Spread shown between. Tightens when MMs are active.
- **News Feed** — every scenario-injected event with a sentiment
  badge (BULL / NEUT / BEAR) and source tag.
- **Agent Feed** — streaming list of every agent action, colored
  by type (PLACE_ORDER, CREATE_POST, LIKE_POST, …). Auto-scrolls;
  if you scroll up to read something, auto-scroll pauses until you
  come back to the bottom.
- **Forecast** — ensemble outcome across multiple seed runs
  (populated after running eval).

### Bottom row — "zoom out"

- **PnL History** — aggregate wealth of all agents combined,
  priced at each step's market price. Tracks how the total pie
  changes as the market moves.
- **Recent Trades** — tape of filled trades with green BUY / red
  SELL chips.
- **Eval Preview** — three scorecard tiers. Click "Open full eval
  page" for the complete scorecard.

### Social Feed (bottom, full-width)

Every agent post with their handle, archetype, content, nested
comments, likes. In LLM mode this reads like a real crypto Twitter
timeline — FOMO degens with rocket emoji, KOLs writing threads,
whales dropping terse one-liners. Scroll freely; the feed won't
hijack your position.

---

## God-mode: inject news while a run is live

While a simulation is still running, you can drop a breaking-news
headline into the feed and watch the agents react. Useful for
stress-testing ("what if the SEC approves an ETF *right now*?").

Ask your admin to wire this up for you — it's available, just not
exposed as a button in the UI yet.

---

## Costs and run times

Rough guide at the default **240 steps × 1,000 agents**:

| Mode | Wall clock | API cost |
|---|---|---|
| Gate-only (heuristic) | **~30 seconds** | **$0** |
| LLM (gpt-4o-mini) | ~8 minutes | ~$1 – $5 |
| LLM (Claude Sonnet 4.6) | ~15 minutes | ~$30 – $50 |

**The cost estimate in the scenario builder updates live** as you change
duration and agent count, so you know what you're signing up for before
clicking RUN.

For fast iteration: use gate-only mode. For a real narrative demo: use
the `llm_quick_demo` scenario — 40 agents × 30 steps, about $0.50 and
2 minutes.

---

## Tips

- **Start with `llm_quick_demo`** if you're new. It runs in 2 minutes
  and produces rich in-voice agent posts so you can see the narrative
  engine working.
- **Use the same seed** when comparing scenarios. Change *one thing*
  (news timing, audience, population mix) and compare — that's the
  cleanest way to learn what each knob does.
- **Watch the Social Feed during replay.** That's where you see
  sentiment form. A KOL post at step 20 → followers reposting at
  step 22 → FOMO degens piling in at step 25 → price top at step
  30. The narrative chain is visible.
- **Trust gate-only for "does the scenario setup make sense," not
  for "will this market actually behave this way."** Real price
  dynamics need LLM agents. Gate-only is a smoke test, not a
  forecast.
- **Try god-mode during a live run.** Run a long gate-only scenario,
  then mid-sim inject a surprise headline. Watch the social feed
  and order book react. This is the single most fun feature once
  you're familiar with the UI.

---

## Quick troubleshooting

- **Charts show `0%` everywhere** — you're looking at a run from
  before we fixed the telemetry. Fire a fresh run; the new one will
  populate correctly.
- **Page shows "WebSocket closed"** — you clicked a link to a
  completed run via the live URL. Refresh, or go back to Home and
  click REPLAY instead of the Run ID.
- **LLM run cost seems too high** — the estimate next to the LLM
  toggle uses your current duration × agent count × 5% active rate
  × $0.003/call. Turn down agents or shorten duration to cut cost
  proportionally.
- **Nothing's happening on screen** — a fresh LLM run takes a minute
  before the first action shows up (agents are thinking). Watch the
  Agent Feed; the first few entries will start streaming once step 1
  completes.

---

## What you can actually do with this

1. **Stress-test narratives.** Got a thesis like "a rate cut will
   pump BTC 5%"? Build the scenario, run 10 seeds, see how often the
   sim agrees.
2. **Explore contagion.** A USDT depeg scenario lets you see how
   stablecoin stress propagates into BTC and ETH holdings across
   agents with different risk profiles.
3. **Study KOL influence.** Run the same news event with audience
   = `kols` vs `all` vs `whales`. Watch the difference in how fast
   the price moves and how the post pattern differs.
4. **Compare agent-population compositions.** What does a 50%-
   paperhands market look like vs a 50%-HODLer one, with the same
   news? Both feel very different in practice.
5. **Replay historical events.** Start the sim at 2022-05-07
   prices with the Luna-depeg calibration scenario; does the sim
   reproduce the real-world cascade?

Every one of these is a point-and-click workflow. The tool is
designed so you spend your time asking interesting market
questions, not wrangling configuration.

---

*Built by [Defily](https://www.defily.ai/) · DeSimulator, 2026*
