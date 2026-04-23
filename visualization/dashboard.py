"""
OASIS Stock Market + Social Media Dashboard

Run with: streamlit run visualization/dashboard.py
"""
import json
import os
import sqlite3
from collections import Counter

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OASIS Simulation Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("OASIS Simulation Dashboard")
st.caption("Social Media + Stock Market Simulation Analysis")


# ── DB selector ──────────────────────────────────────────────────────────────
default_db = os.path.join(os.path.dirname(__file__), "..", "data",
                          "stock_market_demo.db")
db_path = st.sidebar.text_input("Database path", value=default_db)

if not os.path.exists(db_path):
    st.error(f"Database not found: {db_path}")
    st.stop()


@st.cache_data(ttl=5)
def query(sql, params=None):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(sql, conn, params=params or [])
    conn.close()
    return df


# ── Sidebar summary ──────────────────────────────────────────────────────────
companies_df = query("SELECT * FROM company ORDER BY ticker")
users_df = query("SELECT * FROM user")
trades_df = query(
    "SELECT t.*, c.ticker, c.name as company_name, c.sector "
    "FROM trade t JOIN company c ON t.company_id = c.company_id "
    "ORDER BY t.created_at")
orders_df = query(
    "SELECT o.*, c.ticker, c.sector FROM stock_order o "
    "JOIN company c ON o.company_id = c.company_id ORDER BY o.created_at")
trace_df = query("SELECT * FROM trace ORDER BY created_at")
posts_df = query(
    "SELECT p.*, u.name as author FROM post p "
    "JOIN user u ON p.user_id = u.user_id ORDER BY p.created_at DESC")
comments_df = query(
    "SELECT c.*, u.name as author FROM comment c "
    "JOIN user u ON c.user_id = u.user_id ORDER BY c.created_at DESC")

st.sidebar.markdown("---")
st.sidebar.metric("Agents", len(users_df))
st.sidebar.metric("Companies", len(companies_df))
st.sidebar.metric("Total Trades", len(trades_df))
st.sidebar.metric("Total Orders", len(orders_df))
st.sidebar.metric("Social Posts", len(posts_df))
st.sidebar.metric("Comments", len(comments_df))

# ── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "Market Overview", "Trading Activity", "Social Media", "Agent Insights"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: MARKET OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    # ── Price Change Summary ──
    st.subheader("Stock Price Summary")
    if not companies_df.empty:
        price_df = companies_df.copy()
        price_df["current_price"] = price_df["last_price"].fillna(
            price_df["initial_price"])
        price_df["change_pct"] = (
            (price_df["current_price"] - price_df["initial_price"])
            / price_df["initial_price"] * 100
        ).round(2)
        price_df["change"] = price_df["change_pct"].apply(
            lambda x: f"+{x:.1f}%" if x >= 0 else f"{x:.1f}%")

        # Metric cards
        cols = st.columns(min(len(price_df), 6))
        for i, row in price_df.iterrows():
            with cols[i % len(cols)]:
                delta = row["current_price"] - row["initial_price"]
                st.metric(
                    f"{row['ticker']}",
                    f"${row['current_price']:.2f}",
                    f"{delta:+.2f} ({row['change']})",
                )

    # ── Trade Price History ──
    st.subheader("Trade Price History")
    if not trades_df.empty:
        fig = px.scatter(
            trades_df, x="created_at", y="price",
            color="ticker", size="quantity",
            hover_data=["buyer_id", "seller_id", "quantity"],
            title="Executed Trades Over Time",
            labels={"created_at": "Time", "price": "Trade Price ($)"},
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trades executed yet. Run more simulation steps.")

    # ── Order Book Depth ──
    st.subheader("Order Book Depth")
    if not companies_df.empty:
        selected_ticker = st.selectbox(
            "Select company", companies_df["ticker"].tolist(),
            key="ob_ticker")
        company_row = companies_df[
            companies_df["ticker"] == selected_ticker].iloc[0]
        cid = company_row["company_id"]

        bids = query(
            "SELECT price, SUM(quantity - filled_quantity) as vol "
            "FROM stock_order WHERE company_id = ? AND side = 'buy' "
            "AND status = 'open' GROUP BY price ORDER BY price DESC LIMIT 10",
            [cid])
        asks = query(
            "SELECT price, SUM(quantity - filled_quantity) as vol "
            "FROM stock_order WHERE company_id = ? AND side = 'sell' "
            "AND status = 'open' GROUP BY price ORDER BY price ASC LIMIT 10",
            [cid])

        col1, col2 = st.columns(2)
        with col1:
            if not bids.empty:
                fig_bids = go.Figure(go.Bar(
                    y=[f"${p:.2f}" for p in bids["price"]],
                    x=bids["vol"], orientation="h",
                    marker_color="green", name="Bids"))
                fig_bids.update_layout(
                    title="Bids (Buy Orders)", height=300,
                    xaxis_title="Volume", yaxis_title="Price")
                st.plotly_chart(fig_bids, use_container_width=True)
            else:
                st.info("No open buy orders")
        with col2:
            if not asks.empty:
                fig_asks = go.Figure(go.Bar(
                    y=[f"${p:.2f}" for p in asks["price"]],
                    x=asks["vol"], orientation="h",
                    marker_color="red", name="Asks"))
                fig_asks.update_layout(
                    title="Asks (Sell Orders)", height=300,
                    xaxis_title="Volume", yaxis_title="Price")
                st.plotly_chart(fig_asks, use_container_width=True)
            else:
                st.info("No open sell orders")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: TRADING ACTIVITY
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    # ── Volume by Company ──
    st.subheader("Trade Volume by Company")
    if not trades_df.empty:
        vol_df = trades_df.groupby(["ticker", "sector"]).agg(
            total_volume=("quantity", "sum"),
            num_trades=("trade_id", "count"),
            avg_price=("price", "mean"),
        ).reset_index().sort_values("total_volume", ascending=False)

        fig = px.bar(
            vol_df, x="ticker", y="total_volume", color="sector",
            hover_data=["num_trades", "avg_price"],
            title="Total Shares Traded by Company",
            labels={"total_volume": "Shares Traded", "ticker": "Company"},
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trades yet.")

    # ── Order Flow ──
    st.subheader("Order Flow")
    if not orders_df.empty:
        order_summary = orders_df.groupby(["ticker", "side"]).agg(
            count=("order_id", "count"),
            total_qty=("quantity", "sum"),
        ).reset_index()
        fig = px.bar(
            order_summary, x="ticker", y="total_qty", color="side",
            barmode="group",
            color_discrete_map={"buy": "green", "sell": "red"},
            title="Buy vs Sell Order Volume",
            labels={"total_qty": "Total Quantity", "ticker": "Company"},
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    # ── Recent Orders Table ──
    st.subheader("Recent Orders")
    if not orders_df.empty:
        display_cols = ["order_id", "ticker", "side", "price", "quantity",
                        "filled_quantity", "status", "created_at"]
        available = [c for c in display_cols if c in orders_df.columns]
        st.dataframe(
            orders_df[available].tail(30).sort_values(
                "created_at", ascending=False),
            use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: SOCIAL MEDIA
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    # ── Action Breakdown ──
    st.subheader("Agent Action Breakdown")
    if not trace_df.empty:
        action_counts = trace_df["action"].value_counts().reset_index()
        action_counts.columns = ["action", "count"]
        # Exclude internal actions
        exclude = {"sign_up", "exit", "update_rec_table", "refresh",
                   "listen_from_group", "check_portfolio",
                   "view_market_summary", "view_order_book"}
        visible = action_counts[~action_counts["action"].isin(exclude)]

        col1, col2 = st.columns([1, 1])
        with col1:
            fig = px.pie(
                visible, names="action", values="count",
                title="Action Distribution",
                color_discrete_sequence=px.colors.qualitative.Set3,
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Market vs social actions
            market_actions = {"place_order", "cancel_order"}
            social_actions = {"create_post", "create_comment", "like_post",
                              "dislike_post", "repost", "quote_post",
                              "follow", "report_post"}
            market_count = trace_df[
                trace_df["action"].isin(market_actions)].shape[0]
            social_count = trace_df[
                trace_df["action"].isin(social_actions)].shape[0]
            other_count = trace_df[
                ~trace_df["action"].isin(
                    market_actions | social_actions | exclude)].shape[0]

            fig2 = px.pie(
                names=["Social", "Market", "Other"],
                values=[social_count, market_count, other_count],
                title="Social vs Market Activity",
                color_discrete_sequence=["#636EFA", "#EF553B", "#00CC96"],
            )
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)

    # ── Post Feed ──
    st.subheader("Social Media Feed")
    if not posts_df.empty:
        for _, post in posts_df.head(20).iterrows():
            with st.container():
                header = f"**@{post['author']}**"
                likes = post.get("num_likes", 0)
                dislikes = post.get("num_dislikes", 0)
                shares = post.get("num_shares", 0)
                content = post["content"][:500]

                # Count comments for this post
                n_comments = len(
                    comments_df[comments_df["post_id"] == post["post_id"]]
                ) if not comments_df.empty else 0

                st.markdown(f"{header} &nbsp; *{post['created_at']}*")
                st.markdown(f"> {content}")
                st.caption(
                    f"👍 {likes}  👎 {dislikes}  🔄 {shares}  "
                    f"💬 {n_comments}")
                st.divider()

    # ── Top Keywords ──
    st.subheader("Top Keywords in Posts")
    if not posts_df.empty:
        stop_words = {
            "the", "a", "an", "is", "are", "was", "were", "be", "been",
            "being", "have", "has", "had", "do", "does", "did", "will",
            "would", "could", "should", "may", "might", "shall", "can",
            "to", "of", "in", "for", "on", "with", "at", "by", "from",
            "as", "into", "through", "during", "before", "after", "and",
            "but", "or", "nor", "not", "so", "yet", "both", "either",
            "neither", "each", "every", "all", "any", "few", "more",
            "most", "other", "some", "such", "no", "only", "own", "same",
            "than", "too", "very", "just", "about", "above", "also",
            "i", "you", "he", "she", "it", "we", "they", "me", "him",
            "her", "us", "them", "my", "your", "his", "its", "our",
            "their", "this", "that", "these", "those", "i'm", "i'd",
            "what", "which", "who", "whom", "when", "where", "how",
            "if", "then", "else", "while", "because", "although",
        }
        all_words = " ".join(posts_df["content"].str.lower()).split()
        word_counts = Counter(
            w.strip(".,!?\"'()[]{}:;") for w in all_words
            if len(w) > 3 and w.lower().strip(".,!?\"'()[]{}:;")
            not in stop_words
        )
        top_words = pd.DataFrame(
            word_counts.most_common(20), columns=["word", "count"])
        if not top_words.empty:
            fig = px.bar(
                top_words, x="count", y="word", orientation="h",
                title="Most Frequent Words in Posts",
                color="count", color_continuous_scale="blues",
            )
            fig.update_layout(height=500, yaxis={"autorange": "reversed"})
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: AGENT INSIGHTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab4:
    # ── Portfolio Leaderboard ──
    st.subheader("Portfolio Leaderboard")
    portfolio_df = query(
        "SELECT u.user_id, u.name, w.cash, "
        "COALESCE(SUM(p.shares * COALESCE(c.last_price, c.initial_price)), 0)"
        " as stock_value "
        "FROM user u "
        "JOIN wallet w ON u.user_id = w.user_id "
        "LEFT JOIN portfolio p ON u.user_id = p.user_id AND p.shares > 0 "
        "LEFT JOIN company c ON p.company_id = c.company_id "
        "GROUP BY u.user_id "
        "ORDER BY (w.cash + COALESCE(SUM(p.shares * "
        "COALESCE(c.last_price, c.initial_price)), 0)) DESC")

    if not portfolio_df.empty:
        portfolio_df["total_value"] = (
            portfolio_df["cash"] + portfolio_df["stock_value"])
        portfolio_df["cash_pct"] = (
            portfolio_df["cash"] / portfolio_df["total_value"] * 100).round(1)

        # Top and bottom
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Top 10 Portfolios**")
            top10 = portfolio_df.head(10).copy()
            top10["total_value"] = top10["total_value"].apply(
                lambda x: f"${x:,.2f}")
            top10["cash"] = top10["cash"].apply(lambda x: f"${x:,.2f}")
            top10["stock_value"] = top10["stock_value"].apply(
                lambda x: f"${x:,.2f}")
            st.dataframe(
                top10[["name", "cash", "stock_value", "total_value"]],
                use_container_width=True, hide_index=True)

        with col2:
            st.markdown("**Bottom 10 Portfolios**")
            bottom10 = portfolio_df.tail(10).copy()
            bottom10["total_value"] = bottom10["total_value"].apply(
                lambda x: f"${x:,.2f}")
            bottom10["cash"] = bottom10["cash"].apply(lambda x: f"${x:,.2f}")
            bottom10["stock_value"] = bottom10["stock_value"].apply(
                lambda x: f"${x:,.2f}")
            st.dataframe(
                bottom10[["name", "cash", "stock_value", "total_value"]],
                use_container_width=True, hide_index=True)

        # Cash vs Stock bar chart
        st.subheader("Cash vs Stock Allocation")
        chart_df = portfolio_df.head(20).copy()
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Cash", x=chart_df["name"], y=chart_df["cash"],
            marker_color="#636EFA"))
        fig.add_trace(go.Bar(
            name="Stocks", x=chart_df["name"], y=chart_df["stock_value"],
            marker_color="#EF553B"))
        fig.update_layout(
            barmode="stack", height=400,
            title="Portfolio Composition (Top 20 Agents)",
            xaxis_title="Agent", yaxis_title="Value ($)",
            xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Most Active Traders ──
    st.subheader("Most Active Traders")
    trader_df = query(
        "SELECT u.name, "
        "SUM(CASE WHEN t.action = 'place_order' THEN 1 ELSE 0 END) "
        "as orders_placed, "
        "SUM(CASE WHEN t.action = 'create_post' THEN 1 ELSE 0 END) "
        "as posts_created, "
        "SUM(CASE WHEN t.action = 'create_comment' THEN 1 ELSE 0 END) "
        "as comments_made, "
        "SUM(CASE WHEN t.action = 'like_post' THEN 1 ELSE 0 END) "
        "as likes_given, "
        "COUNT(*) as total_actions "
        "FROM trace t JOIN user u ON t.user_id = u.user_id "
        "GROUP BY u.user_id ORDER BY orders_placed DESC")

    if not trader_df.empty:
        fig = px.bar(
            trader_df.head(15), x="name", y=["orders_placed",
                                              "posts_created",
                                              "comments_made",
                                              "likes_given"],
            title="Agent Activity Breakdown (Top 15)",
            barmode="stack", height=400,
            labels={"value": "Count", "name": "Agent"},
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Trade Log ──
    st.subheader("Trade Log")
    if not trace_df.empty:
        order_traces = trace_df[
            trace_df["action"] == "place_order"].copy()
        if not order_traces.empty:
            parsed = []
            for _, row in order_traces.iterrows():
                try:
                    info = json.loads(row["info"])
                    parsed.append({
                        "user_id": row["user_id"],
                        "time": row["created_at"],
                        "ticker": info.get("ticker", ""),
                        "side": info.get("side", ""),
                        "price": info.get("price", 0),
                        "quantity": info.get("quantity", 0),
                        "trades_filled": len(info.get("trades", [])),
                    })
                except (json.JSONDecodeError, KeyError):
                    pass
            if parsed:
                parsed_df = pd.DataFrame(parsed)
                # Join with user names
                name_map = dict(
                    zip(users_df["user_id"], users_df["name"]))
                parsed_df["agent"] = parsed_df["user_id"].map(name_map)
                st.dataframe(
                    parsed_df[["time", "agent", "side", "ticker",
                               "price", "quantity", "trades_filled"]],
                    use_container_width=True, hide_index=True)
