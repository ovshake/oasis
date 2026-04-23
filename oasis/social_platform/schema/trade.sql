CREATE TABLE IF NOT EXISTS trade (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    buy_order_id INTEGER NOT NULL,
    sell_order_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL,
    price REAL NOT NULL,
    quantity INTEGER NOT NULL,
    buyer_id INTEGER NOT NULL,
    seller_id INTEGER NOT NULL,
    created_at DATETIME NOT NULL,
    FOREIGN KEY(buy_order_id) REFERENCES stock_order(order_id),
    FOREIGN KEY(sell_order_id) REFERENCES stock_order(order_id),
    FOREIGN KEY(company_id) REFERENCES company(company_id)
);
