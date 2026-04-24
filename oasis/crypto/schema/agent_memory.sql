-- Schema definition for the agent_memory table
CREATE TABLE IF NOT EXISTS agent_memory (
    memory_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    step INTEGER NOT NULL,
    kind TEXT,
    content_json TEXT,
    created_at DATETIME NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(user_id) REFERENCES user(user_id)
);

CREATE INDEX IF NOT EXISTS idx_memory_user_step ON agent_memory(user_id, step);
