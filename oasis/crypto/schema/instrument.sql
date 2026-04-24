-- Schema definition for the instrument table
CREATE TABLE IF NOT EXISTS instrument (
    instrument_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    asset_class TEXT NOT NULL CHECK(asset_class IN ('crypto', 'stablecoin', 'commodity', 'fiat')),
    decimals INTEGER NOT NULL DEFAULT 8,
    total_supply REAL,
    peg_target REAL,
    is_quote_asset INTEGER NOT NULL DEFAULT 0,
    yfinance_ticker TEXT,
    binance_symbol TEXT,
    default_price REAL,
    metadata_json TEXT,
    created_at DATETIME NOT NULL DEFAULT (datetime('now'))
);
