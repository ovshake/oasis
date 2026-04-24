-- Schema definition for the persona table
-- persona_id is TEXT (e.g. 'p_000001'), not INTEGER autoincrement.
CREATE TABLE IF NOT EXISTS persona (
    persona_id TEXT PRIMARY KEY,
    archetype TEXT,
    name TEXT,
    backstory TEXT,
    voice_style TEXT,
    config_json TEXT,
    generated_by TEXT,
    created_at DATETIME NOT NULL DEFAULT (datetime('now'))
);
