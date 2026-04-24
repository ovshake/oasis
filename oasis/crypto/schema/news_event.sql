-- Schema definition for the news_event table
CREATE TABLE IF NOT EXISTS news_event (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    step INTEGER NOT NULL,
    source TEXT,
    audience TEXT NOT NULL DEFAULT 'all',
    content TEXT,
    title TEXT,
    sentiment_valence REAL,
    magnitude TEXT,
    credibility TEXT,
    affected_instruments TEXT,
    created_at DATETIME NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_news_step ON news_event(step);
