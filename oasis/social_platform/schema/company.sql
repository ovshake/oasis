CREATE TABLE IF NOT EXISTS company (
    company_id INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    sector TEXT,
    description TEXT,
    total_shares INTEGER NOT NULL,
    initial_price REAL NOT NULL,
    last_price REAL,
    prev_close_price REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
