-- Schema definition for the pair table
CREATE TABLE IF NOT EXISTS pair (
    pair_id INTEGER PRIMARY KEY AUTOINCREMENT,
    base_instrument_id INTEGER NOT NULL,
    quote_instrument_id INTEGER NOT NULL,
    tick_size REAL NOT NULL DEFAULT 0.01,
    lot_size REAL NOT NULL DEFAULT 0.0001,
    last_price REAL,
    prev_close_price REAL,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    UNIQUE(base_instrument_id, quote_instrument_id),
    FOREIGN KEY(base_instrument_id) REFERENCES instrument(instrument_id),
    FOREIGN KEY(quote_instrument_id) REFERENCES instrument(instrument_id)
);
