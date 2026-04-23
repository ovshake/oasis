# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =========== Copyright 2023 @ CAMEL-AI.org. All Rights Reserved. ===========
"""
Stock Market Simulation with God Mode

This example demonstrates:
1. A social media simulation with an integrated stock exchange
2. Agents that can both discuss news AND trade stocks
3. "God mode" — injecting breaking news to influence agent behavior
4. Auto-generated market news posts when significant price moves occur

The simulation runs in steps:
- Step 1: Seed the social media with a normal discussion topic
- Step 2: GOD MODE — inject a geopolitical news event
- Steps 3-5: Agents react, discuss, and trade based on what they see
- Post-simulation: Analyze trades, portfolios, and price movements
"""
import asyncio
import json
import os
import sqlite3

from camel.models import ModelFactory
from camel.types import ModelPlatformType, ModelType

import oasis
from oasis import (ActionType, LLMAction, ManualAction,
                   generate_reddit_agent_graph)


async def main():
    # =====================================================================
    # 1. Configuration
    # =====================================================================

    # Load company profiles
    with open("./data/market/sp500_companies.json", "r") as f:
        companies = json.load(f)

    market_config = {
        "companies": companies,
        "initial_cash": 100000.0,  # Each agent starts with $100K
    }

    # Define the LLM model
    openai_model = ModelFactory.create(
        model_platform=ModelPlatformType.OPENAI,
        model_type=ModelType.GPT_4O_MINI,
    )

    # Combine social + market actions
    available_actions = (
        ActionType.get_default_reddit_actions()
        + ActionType.get_default_market_actions()
    )

    # =====================================================================
    # 2. Create agents from profiles
    # =====================================================================
    agent_graph = await generate_reddit_agent_graph(
        profile_path="./data/reddit/user_data_36.json",
        model=openai_model,
        available_actions=available_actions,
    )

    # =====================================================================
    # 3. Create environment with stock market
    # =====================================================================
    db_path = "./data/stock_market_simulation.db"
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
    print(f"Simulation initialized with {len(companies)} companies "
          f"and {len(list(agent_graph.get_agents()))} agents.")

    # =====================================================================
    # Step 1: Seed with normal discussion
    # =====================================================================
    print("\n--- Step 1: Seeding social media with discussion ---")
    actions_seed = {
        env.agent_graph.get_agent(0): ManualAction(
            action_type=ActionType.CREATE_POST,
            action_args={
                "content": (
                    "Markets are looking stable today. Tech stocks continue "
                    "their steady climb, and oil prices are holding firm "
                    "around $95/barrel. What are you all investing in?"
                )
            }),
    }
    await env.step(actions_seed)

    # =====================================================================
    # Step 2: GOD MODE — Inject breaking news
    # =====================================================================
    print("\n--- Step 2: GOD MODE — Injecting breaking news ---")
    actions_god_mode = {
        env.agent_graph.get_agent(0): ManualAction(
            action_type=ActionType.CREATE_POST,
            action_args={
                "content": (
                    "BREAKING NEWS: Iran has launched a military strike on "
                    "UAE oil infrastructure. Multiple refineries hit. Oil "
                    "supply from the Gulf is severely disrupted. Global "
                    "markets are reacting. This is a developing situation."
                )
            }),
    }
    await env.step(actions_god_mode)

    # =====================================================================
    # Steps 3-5: Agents react with both social and market actions
    # =====================================================================
    for step_num in range(3, 6):
        print(f"\n--- Step {step_num}: Agents react (LLM actions) ---")
        actions_llm = {
            agent: LLMAction()
            for _, agent in env.agent_graph.get_agents()
        }
        await env.step(actions_llm)

    # =====================================================================
    # Close and analyze
    # =====================================================================
    await env.close()

    # =====================================================================
    # Post-simulation analysis
    # =====================================================================
    print("\n" + "=" * 60)
    print("POST-SIMULATION ANALYSIS")
    print("=" * 60)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Price changes
    print("\n--- Stock Price Changes ---")
    cursor.execute(
        "SELECT ticker, name, sector, initial_price, "
        "COALESCE(last_price, initial_price) as current_price "
        "FROM company ORDER BY ticker")
    for row in cursor.fetchall():
        ticker, name, sector, initial, current = row
        change = ((current - initial) / initial) * 100
        print(f"  {ticker:5s} ({sector:25s}): "
              f"${initial:.2f} -> ${current:.2f} ({change:+.1f}%)")

    # Trade volume by company
    print("\n--- Trade Volume ---")
    cursor.execute(
        "SELECT c.ticker, c.sector, COUNT(t.trade_id), "
        "COALESCE(SUM(t.quantity), 0) "
        "FROM company c LEFT JOIN trade t ON c.company_id = t.company_id "
        "GROUP BY c.company_id ORDER BY SUM(t.quantity) DESC")
    for row in cursor.fetchall():
        ticker, sector, num_trades, volume = row
        print(f"  {ticker:5s} ({sector:25s}): "
              f"{num_trades} trades, {volume} shares")

    # Top trading agents
    print("\n--- Most Active Traders ---")
    cursor.execute(
        "SELECT u.name, COUNT(*) as trades "
        "FROM trace t JOIN user u ON t.user_id = u.user_id "
        "WHERE t.action = 'place_order' "
        "GROUP BY t.user_id ORDER BY trades DESC LIMIT 10")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} orders placed")

    # Portfolio value changes
    print("\n--- Portfolio Values (Top 10) ---")
    cursor.execute(
        "SELECT u.name, w.cash, "
        "COALESCE(SUM(p.shares * COALESCE(c.last_price, c.initial_price)), 0)"
        " as stock_value "
        "FROM user u JOIN wallet w ON u.user_id = w.user_id "
        "LEFT JOIN portfolio p ON u.user_id = p.user_id AND p.shares > 0 "
        "LEFT JOIN company c ON p.company_id = c.company_id "
        "GROUP BY u.user_id "
        "ORDER BY (w.cash + COALESCE(SUM(p.shares * "
        "COALESCE(c.last_price, c.initial_price)), 0)) DESC LIMIT 10")
    for row in cursor.fetchall():
        name, cash, stock_val = row
        total = cash + stock_val
        print(f"  {name}: Cash ${cash:,.2f} + Stocks ${stock_val:,.2f} "
              f"= Total ${total:,.2f}")

    # Social media posts about the crisis
    print("\n--- Social Media Discussion (sample) ---")
    cursor.execute(
        "SELECT u.name, p.content FROM post p "
        "JOIN user u ON p.user_id = u.user_id "
        "ORDER BY p.created_at DESC LIMIT 10")
    for row in cursor.fetchall():
        content = row[1][:100] + "..." if len(row[1]) > 100 else row[1]
        print(f"  [{row[0]}]: {content}")

    conn.close()
    print(f"\nFull results saved to: {db_path}")
    print("Use oasis.print_db_contents() or sqlite3 to explore further.")


if __name__ == "__main__":
    asyncio.run(main())
