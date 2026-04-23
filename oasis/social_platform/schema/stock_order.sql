CREATE TABLE IF NOT EXISTS stock_order (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('buy', 'sell')),
    price REAL NOT NULL,
    quantity INTEGER NOT NULL,
    filled_quantity INTEGER DEFAULT 0,
    status TEXT DEFAULT 'open' CHECK(status IN ('open', 'filled', 'cancelled')),
    created_at DATETIME NOT NULL,
    FOREIGN KEY(user_id) REFERENCES user(user_id),
    FOREIGN KEY(company_id) REFERENCES company(company_id)
);
