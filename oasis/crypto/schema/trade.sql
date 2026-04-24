-- Schema definition for the trade table
CREATE TABLE IF NOT EXISTS trade (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id INTEGER NOT NULL,
    buy_order_id INTEGER NOT NULL,
    sell_order_id INTEGER NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    buyer_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    step INTEGER NOT NULL,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(pair_id) REFERENCES pair(pair_id),
    FOREIGN KEY(buy_order_id) REFERENCES crypto_order(order_id),
    FOREIGN KEY(sell_order_id) REFERENCES crypto_order(order_id),
    FOREIGN KEY(buyer_id) REFERENCES user(user_id),
    FOREIGN KEY(seller_id) REFERENCES user(user_id)
);

CREATE INDEX IF NOT EXISTS idx_trade_pair_step ON trade(pair_id, step);
