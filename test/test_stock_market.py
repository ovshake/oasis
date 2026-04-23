"""Tests for the stock market extension: order matching, portfolios, etc."""
import asyncio
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from oasis.social_platform.channel import Channel
from oasis.social_platform.platform import Platform


async def test_market_operations():
    """Test the core market operations: register, initialize, trade, cancel."""
    # Create a temp DB
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = tmp.name
    tmp.close()
    os.environ["OASIS_DB_PATH"] = os.path.abspath(db_path)

    channel = Channel()
    platform = Platform(
        db_path=db_path,
        channel=channel,
        recsys_type="reddit",
        market_news_threshold=0.05,
    )

    # Start platform in background
    platform_task = asyncio.create_task(platform.running())

    # Sign up 3 agents
    for i in range(3):
        msg_id = await channel.write_to_receive_queue(
            (i, (f"user{i}", f"Agent {i}", f"Bio {i}"), "sign_up"))
        await channel.read_from_send_queue(msg_id)

    # Register 2 companies
    companies = [
        {"company_id": 1, "ticker": "AAPL", "name": "Apple Corp",
         "sector": "Technology", "description": "Tech company",
         "total_shares": 90, "initial_price": 100.0},
        {"company_id": 2, "ticker": "OIL", "name": "Oil Corp",
         "sector": "Energy", "description": "Oil company",
         "total_shares": 90, "initial_price": 50.0},
    ]
    for c in companies:
        result = await platform.register_company(c)
        assert result["success"], f"Failed to register {c['ticker']}: {result}"

    # Initialize market: 3 agents, $10000 each, 30 shares each
    result = await platform.initialize_market([0, 1, 2], companies, 10000.0)
    assert result["success"], f"Market init failed: {result}"

    # Verify initial state
    print("=== Test 1: Initial State ===")
    portfolio = await platform.check_portfolio(0)
    assert portfolio["success"]
    assert portfolio["cash"] == 10000.0
    print(f"  Agent 0 cash: ${portfolio['cash']}")
    for h in portfolio["holdings"]:
        print(f"  Agent 0 holds {h['shares']} shares of {h['ticker']}")
        assert h["shares"] == 30  # 90 / 3 agents

    # Test 2: Place a sell order
    print("\n=== Test 2: Sell Order ===")
    msg_id = await channel.write_to_receive_queue(
        (0, ("AAPL", "sell", 105.0, 10), "place_order"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert result["success"], f"Sell order failed: {result}"
    print(f"  Sell order placed: {result['order_id']}, trades: {result['trades']}")
    assert len(result["trades"]) == 0  # No matching buy yet

    # Check shares were escrowed
    portfolio = await platform.check_portfolio(0)
    aapl_holding = [h for h in portfolio["holdings"] if h["ticker"] == "AAPL"]
    assert aapl_holding[0]["shares"] == 20  # 30 - 10 escrowed
    print(f"  Agent 0 AAPL shares after sell escrow: {aapl_holding[0]['shares']}")

    # Test 3: Place a matching buy order
    print("\n=== Test 3: Matching Buy Order ===")
    msg_id = await channel.write_to_receive_queue(
        (1, ("AAPL", "buy", 110.0, 5), "place_order"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert result["success"], f"Buy order failed: {result}"
    assert len(result["trades"]) == 1  # Should match!
    trade = result["trades"][0]
    assert trade["trade_price"] == 105.0  # Maker (sell) price
    assert trade["quantity"] == 5
    print(f"  Trade executed: {trade['quantity']} shares @ ${trade['trade_price']}")

    # Verify buyer got shares and seller got cash
    p0 = await platform.check_portfolio(0)  # seller
    p1 = await platform.check_portfolio(1)  # buyer

    # Seller: started $10000, received 5 * $105 = $525
    assert p0["cash"] == 10000.0 + 5 * 105.0
    print(f"  Seller cash: ${p0['cash']} (expected $10525)")

    # Buyer: started $10000, escrowed 5 * $110 = $550, refunded 5 * ($110-$105) = $25
    # So cash = $10000 - $550 + $25 = $9475
    assert p1["cash"] == 10000.0 - 5 * 110.0 + 5 * (110.0 - 105.0)
    print(f"  Buyer cash: ${p1['cash']} (expected $9475)")

    # Buyer should now have 35 AAPL shares (30 + 5)
    aapl_buyer = [h for h in p1["holdings"] if h["ticker"] == "AAPL"]
    assert aapl_buyer[0]["shares"] == 35
    print(f"  Buyer AAPL shares: {aapl_buyer[0]['shares']} (expected 35)")

    # Test 4: View order book (sell order should have 5 remaining)
    print("\n=== Test 4: Order Book ===")
    msg_id = await channel.write_to_receive_queue(
        (0, "AAPL", "view_order_book"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert result["success"]
    assert result["last_price"] == 105.0  # Last trade
    assert len(result["asks"]) == 1  # Remaining sell
    assert result["asks"][0]["quantity"] == 5  # 10 - 5 filled
    assert len(result["bids"]) == 0  # Buy was fully filled
    print(f"  Last price: ${result['last_price']}")
    print(f"  Bids: {result['bids']}")
    print(f"  Asks: {result['asks']}")
    print(f"  Volume: {result['volume']}")

    # Test 5: Partial fill
    print("\n=== Test 5: Partial Fill ===")
    msg_id = await channel.write_to_receive_queue(
        (2, ("AAPL", "buy", 105.0, 3), "place_order"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert result["success"]
    assert len(result["trades"]) == 1
    assert result["trades"][0]["quantity"] == 3
    print(f"  Partial fill: {result['trades'][0]['quantity']} of 3 shares")

    # Test 6: Cancel order
    print("\n=== Test 6: Cancel Order ===")
    # Place a new buy order that won't match
    msg_id = await channel.write_to_receive_queue(
        (1, ("AAPL", "buy", 90.0, 10), "place_order"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert result["success"]
    order_id = result["order_id"]
    p1_before = await platform.check_portfolio(1)

    # Cancel it
    msg_id = await channel.write_to_receive_queue(
        (1, order_id, "cancel_order"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert result["success"]
    print(f"  Cancelled order {order_id}")

    # Cash should be refunded
    p1_after = await platform.check_portfolio(1)
    assert p1_after["cash"] == p1_before["cash"] + 90.0 * 10
    print(f"  Cash refunded: ${p1_after['cash'] - p1_before['cash']}")

    # Test 7: Insufficient funds
    print("\n=== Test 7: Insufficient Funds ===")
    msg_id = await channel.write_to_receive_queue(
        (0, ("AAPL", "buy", 100.0, 999999), "place_order"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert not result["success"]
    assert "Insufficient cash" in result["error"]
    print(f"  Correctly rejected: {result['error']}")

    # Test 8: Insufficient shares
    print("\n=== Test 8: Insufficient Shares ===")
    msg_id = await channel.write_to_receive_queue(
        (0, ("AAPL", "sell", 100.0, 999), "place_order"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert not result["success"]
    assert "Insufficient shares" in result["error"]
    print(f"  Correctly rejected: {result['error']}")

    # Test 9: Market summary
    print("\n=== Test 9: Market Summary ===")
    msg_id = await channel.write_to_receive_queue(
        (0, None, "view_market_summary"))
    result = (await channel.read_from_send_queue(msg_id))[2]
    assert result["success"]
    for c in result["companies"]:
        print(f"  {c['ticker']}: ${c['last_price']} ({c['change_pct']:+.1f}%)")

    # Test 10: Cash/share accounting integrity
    print("\n=== Test 10: Accounting Integrity ===")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Total cash in wallets + escrowed in open buy orders should equal initial
    cursor.execute("SELECT SUM(cash) FROM wallet")
    total_wallet_cash = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COALESCE(SUM(price * (quantity - filled_quantity)), 0) "
        "FROM stock_order WHERE side = 'buy' AND status = 'open'")
    escrowed_cash = cursor.fetchone()[0]
    initial_total_cash = 3 * 10000.0  # 3 agents * $10000
    # Add cash received from trades by sellers
    total_cash_in_system = total_wallet_cash + escrowed_cash
    print(f"  Total wallet cash: ${total_wallet_cash:.2f}")
    print(f"  Escrowed in buy orders: ${escrowed_cash:.2f}")
    print(f"  System total: ${total_cash_in_system:.2f} "
          f"(initial: ${initial_total_cash:.2f})")
    # Note: total cash is conserved — money only moves between wallets
    assert abs(total_cash_in_system - initial_total_cash) < 0.01, \
        f"Cash leak! {total_cash_in_system} != {initial_total_cash}"

    # Total shares per company should equal total_shares
    for company in companies:
        cursor.execute(
            "SELECT COALESCE(SUM(shares), 0) FROM portfolio "
            "WHERE company_id = ?", (company["company_id"],))
        held_shares = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COALESCE(SUM(quantity - filled_quantity), 0) "
            "FROM stock_order WHERE company_id = ? AND side = 'sell' "
            "AND status = 'open'", (company["company_id"],))
        escrowed_shares = cursor.fetchone()[0]
        total = held_shares + escrowed_shares
        print(f"  {company['ticker']}: {held_shares} held + "
              f"{escrowed_shares} escrowed = {total} "
              f"(expected {company['total_shares']})")
        assert total == company["total_shares"], \
            f"Share leak for {company['ticker']}! {total} != {company['total_shares']}"

    conn.close()

    # Shutdown
    await channel.write_to_receive_queue((None, None, "exit"))
    await platform_task

    # Cleanup
    os.unlink(db_path)
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    asyncio.run(test_market_operations())
