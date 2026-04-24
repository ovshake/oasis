-- Schema definition for the crypto_order table
-- Named crypto_order (not "order") because ORDER is a SQL reserved word.
CREATE TABLE IF NOT EXISTS crypto_order (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    pair_id INTEGER NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
    order_type TEXT NOT NULL CHECK(order_type IN ('limit', 'market')) DEFAULT 'limit',
    price REAL,
    quantity REAL NOT NULL,
    filled_quantity REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK(status IN ('open', 'filled', 'cancelled')) DEFAULT 'open',
    step INTEGER NOT NULL,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(user_id) REFERENCES user(user_id),
    FOREIGN KEY(pair_id) REFERENCES pair(pair_id)
);

CREATE INDEX IF NOT EXISTS idx_crypto_order_pair_side_status_price
    ON crypto_order(pair_id, side, status, price);
