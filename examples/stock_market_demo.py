"""
Stock Market Simulation — Cranked Up Edition

Heavy news flow, seeded liquidity on both sides, randomized events.
Visualize results with: streamlit run visualization/dashboard.py
"""
import asyncio
import json
import os
import random
import sqlite3

from camel.models import ModelFactory
from camel.types import ModelPlatformType, ModelType

import oasis
from oasis import (ActionType, LLMAction, ManualAction,
                   generate_reddit_agent_graph)

# ── News event pools ─────────────────────────────────────────────────────────
CRISIS_EVENTS = [
    ("Iran launches military strike on UAE oil refineries. Persian Gulf "
     "shipping lanes threatened. Oil futures spiking 15%. Pentagon on alert.",
     "Oil crisis — Iran strikes UAE"),
    ("Massive earthquake hits Taiwan semiconductor hub. TSMC fabs offline. "
     "Global chip shortage expected. Tech supply chain in chaos.",
     "Tech crisis — Taiwan earthquake"),
    ("Federal Reserve announces emergency 75bp rate hike. Inflation running "
     "at 9.2%. Banking stocks tumbling. Mortgage rates surging.",
     "Rate shock — Fed emergency hike"),
    ("Major cyberattack takes down US financial infrastructure. JP Morgan, "
     "Goldman Sachs systems offline. Treasury investigating.",
     "Cyber crisis — financial systems down"),
    ("Saudi Arabia slashes oil production by 3M barrels/day. OPEC+ members "
     "follow suit. Energy prices set to surge globally.",
     "OPEC shock — production cut"),
    ("China announces military exercises around Taiwan. US deploys carrier "
     "groups. Rare earth mineral exports halted. Defense stocks surging.",
     "Geopolitical — China-Taiwan tensions"),
]

POSITIVE_EVENTS = [
    ("Nova Digital (NVDA) announces 10x more efficient AI chip. Major cloud "
     "providers placing massive pre-orders. Analysts call it generational.",
     "Tech boom — NVDA breakthrough"),
    ("Ceasefire announced in Middle East. Oil supply routes reopening. "
     "Energy analysts say prices normalize within weeks. Relief rally.",
     "Ceasefire — oil supply resuming"),
    ("Fed signals rate cuts coming. Inflation dropping to 2.1%. Markets "
     "rally across the board. Consumer confidence at 5-year high.",
     "Fed pivot — rate cuts coming"),
    ("Major oil discovery in Gulf of Mexico. Estimated 5B barrels. US "
     "energy independence boosted. XOM and CVX lead exploration.",
     "Energy boom — massive oil discovery"),
    ("DefenseNet (DFN) wins $50B Pentagon contract for next-gen missile "
     "defense system. Largest defense contract in decade.",
     "Defense win — $50B contract"),
    ("Universal Air Lines (UAL) reports record bookings. Summer travel "
     "demand unprecedented. Fuel costs stable. Airlines sector surging.",
     "Airlines boom — record demand"),
]

RUMORS_AND_NOISE = [
    "Hearing rumors that JP Financial might acquire a major fintech startup. "
    "Could be huge for the financial sector. Anyone else seeing this?",
    "My sources say NVDA's new chip has serious overheating issues. Could "
    "delay launch by 6 months. Not confirmed yet but worth watching.",
    "Oil traders on Twitter saying inventories are at historic lows. Could "
    "see $150/barrel if any supply disruption happens. Scary times.",
    "Just read that defense spending bill passed committee. DFN and other "
    "defense contractors should see major boosts next quarter.",
    "Airlines are secretly hedging fuel at $120/barrel. They expect oil to "
    "spike. Bad sign for UAL and other carriers if true.",
    "Insider tip: rare earth metal shortage incoming. China restricting "
    "exports. This affects everything from chips to EVs to defense.",
]


async def main():
    # ── Config ───────────────────────────────────────────────────────────────
    with open("./data/market/sp500_companies.json", "r") as f:
        all_companies = json.load(f)

    demo_tickers = {"NVDA", "XOM", "CVX", "UAL", "DFN", "JPF"}
    companies = [c for c in all_companies if c["ticker"] in demo_tickers]

    market_config = {
        "companies": companies,
        "initial_cash": 50000.0,
    }

    model = ModelFactory.create(
        model_platform=ModelPlatformType.ANTHROPIC,
        model_type=ModelType.CLAUDE_3_HAIKU,
    )

    available_actions = (
        ActionType.get_default_reddit_actions()
        + ActionType.get_default_market_actions()
    )

    agent_graph = await generate_reddit_agent_graph(
        profile_path="./data/reddit/user_data_36.json",
        model=model,
        available_actions=available_actions,
    )

    db_path = "./data/stock_market_demo.db"
    os.environ["OASIS_DB_PATH"] = os.path.abspath(db_path)
    if os.path.exists(db_path):
        os.remove(db_path)

    env = oasis.make(
        agent_graph=agent_graph,
        platform=oasis.DefaultPlatformType.REDDIT,
        database_path=db_path,
        market_config=market_config,
    )

    await env.reset()
    num_agents = len(list(agent_graph.get_agents()))
    print(f"Initialized: {len(companies)} companies, {num_agents} agents, "
          f"$50K cash each\n")

    # Helpers
    async def llm_step(label):
        print(f"\n{'=' * 60}\n{label}\n{'=' * 60}")
        actions = {
            agent: LLMAction()
            for _, agent in env.agent_graph.get_agents()
        }
        await env.step(actions)

    async def inject_news(agent_id, content, label):
        print(f"\n{'=' * 60}\nGOD MODE: {label}\n{'=' * 60}")
        await env.step({
            env.agent_graph.get_agent(agent_id): ManualAction(
                action_type=ActionType.CREATE_POST,
                action_args={"content": content}),
        })

    async def seed_orders(agents_range, side, price_mult, qty):
        """Seed buy or sell orders across companies for liquidity."""
        actions = {}
        tickers = [c["ticker"] for c in companies]
        prices = {c["ticker"]: c["initial_price"] for c in companies}
        for i, idx in enumerate(agents_range):
            agent = env.agent_graph.get_agent(idx)
            ticker = tickers[i % len(tickers)]
            price = round(prices[ticker] * price_mult, 2)
            actions[agent] = ManualAction(
                action_type=ActionType.PLACE_ORDER,
                action_args={
                    "ticker": ticker, "side": side,
                    "price": price, "quantity": qty,
                })
        await env.step(actions)

    # ═══════════════════════════════════════════════════════════════════════════
    # SEED LIQUIDITY — both sides
    # ═══════════════════════════════════════════════════════════════════════════
    print("Seeding market liquidity...")
    # Sell orders: agents 1-12 at 3-8% above initial
    await seed_orders(range(1, 7), "sell", 1.03, 5)
    await seed_orders(range(7, 13), "sell", 1.06, 5)
    await seed_orders(range(13, 19), "sell", 1.10, 5)
    # Buy orders: agents 19-30 at 2-5% below initial
    await seed_orders(range(19, 25), "buy", 0.97, 5)
    await seed_orders(range(25, 31), "buy", 0.95, 5)
    print("Seeded 30 sell + 12 buy orders.\n")

    # ═══════════════════════════════════════════════════════════════════════════
    # SIMULATION TIMELINE — randomized news flow
    # ═══════════════════════════════════════════════════════════════════════════
    step = 0

    # Phase 1: Calm before the storm
    await inject_news(0, (
        "Good morning traders! Markets opened flat. Tech holding steady, "
        "oil at $95, defense quiet. Feels like the calm before something "
        "big. What's your read on the market?"
    ), "Market open — calm start")
    step += 1

    await llm_step(f"STEP {step}: Normal reactions")
    step += 1

    # Phase 2: First crisis
    crisis1 = random.choice(CRISIS_EVENTS)
    await inject_news(0, f"BREAKING: {crisis1[0]}", crisis1[1])
    step += 1

    await llm_step(f"STEP {step}: Crisis reaction wave 1")
    step += 1
    await llm_step(f"STEP {step}: Crisis reaction wave 2")
    step += 1

    # Phase 3: Rumor mill
    rumor = random.choice(RUMORS_AND_NOISE)
    await inject_news(
        random.randint(1, 5), rumor, "Market rumor spreading")
    step += 1

    await llm_step(f"STEP {step}: Rumor reactions")
    step += 1

    # Phase 4: Positive catalyst
    positive1 = random.choice(POSITIVE_EVENTS)
    await inject_news(0, f"BREAKING: {positive1[0]}", positive1[1])
    step += 1

    await llm_step(f"STEP {step}: Positive catalyst reactions")
    step += 1
    await llm_step(f"STEP {step}: Continued rebalancing")
    step += 1

    # Phase 5: Second crisis or reversal
    crisis2 = random.choice(
        [e for e in CRISIS_EVENTS if e != crisis1])
    await inject_news(0, f"BREAKING: {crisis2[0]}", crisis2[1])
    step += 1

    await llm_step(f"STEP {step}: Second crisis reactions")
    step += 1

    # Phase 6: Another positive + rumor combo
    positive2 = random.choice(
        [e for e in POSITIVE_EVENTS if e != positive1])
    rumor2 = random.choice(
        [r for r in RUMORS_AND_NOISE if r != rumor])
    await inject_news(0, f"BREAKING: {positive2[0]}", positive2[1])
    await inject_news(
        random.randint(1, 5), rumor2, "Second rumor wave")
    step += 1

    await llm_step(f"STEP {step}: Final trading frenzy")
    step += 1
    await llm_step(f"STEP {step}: Market close reactions")
    step += 1

    await env.close()

    # ═══════════════════════════════════════════════════════════════════════════
    # ANALYSIS
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 60)
    print("POST-SIMULATION ANALYSIS")
    print("=" * 60)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\n--- Stock Price Changes ---")
    cursor.execute(
        "SELECT ticker, name, sector, initial_price, "
        "COALESCE(last_price, initial_price) as price FROM company "
        "ORDER BY (COALESCE(last_price, initial_price) - initial_price) "
        "/ initial_price DESC")
    for row in cursor.fetchall():
        ticker, name, sector, initial, price = row
        change = ((price - initial) / initial) * 100
        arrow = "^" if change > 0 else ("v" if change < 0 else "=")
        print(f"  {arrow} {ticker:5s} {name:25s} ({sector:15s}): "
              f"${initial:.2f} -> ${price:.2f} ({change:+.1f}%)")

    print("\n--- Trade Activity ---")
    cursor.execute(
        "SELECT c.ticker, c.sector, COUNT(t.trade_id), "
        "COALESCE(SUM(t.quantity), 0) "
        "FROM company c LEFT JOIN trade t ON c.company_id = t.company_id "
        "GROUP BY c.company_id ORDER BY COUNT(t.trade_id) DESC")
    for row in cursor.fetchall():
        print(f"  {row[0]:5s} ({row[1]:15s}): {row[2]} trades, "
              f"{row[3]} shares")

    print("\n--- Order Book Snapshot ---")
    cursor.execute("SELECT DISTINCT ticker FROM company ORDER BY ticker")
    tickers = [r[0] for r in cursor.fetchall()]
    for ticker in tickers:
        cursor.execute(
            "SELECT c.company_id, COALESCE(c.last_price, c.initial_price) "
            "FROM company c WHERE c.ticker = ?", (ticker,))
        cid, last_price = cursor.fetchone()
        cursor.execute(
            "SELECT price, SUM(quantity - filled_quantity) FROM stock_order "
            "WHERE company_id = ? AND side = 'buy' AND status = 'open' "
            "GROUP BY price ORDER BY price DESC LIMIT 3", (cid,))
        bids = cursor.fetchall()
        cursor.execute(
            "SELECT price, SUM(quantity - filled_quantity) FROM stock_order "
            "WHERE company_id = ? AND side = 'sell' AND status = 'open' "
            "GROUP BY price ORDER BY price ASC LIMIT 3", (cid,))
        asks = cursor.fetchall()
        bid_str = " | ".join(f"${b[0]:.0f}x{b[1]}" for b in bids) or "empty"
        ask_str = " | ".join(f"${a[0]:.0f}x{a[1]}" for a in asks) or "empty"
        print(f"  {ticker} (${last_price:.2f}):  "
              f"BIDS [{bid_str}]  ASKS [{ask_str}]")

    cursor.execute("SELECT COUNT(*) FROM trade")
    total_trades = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM stock_order")
    total_orders = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM post")
    total_posts = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM comment")
    total_comments = cursor.fetchone()[0]

    print(f"\n--- Summary ---")
    print(f"  Total trades:  {total_trades}")
    print(f"  Total orders:  {total_orders}")
    print(f"  Social posts:  {total_posts}")
    print(f"  Comments:      {total_comments}")

    conn.close()
    print(f"\nResults saved to: {db_path}")
    print("Dashboard: streamlit run visualization/dashboard.py")


if __name__ == "__main__":
    asyncio.run(main())
