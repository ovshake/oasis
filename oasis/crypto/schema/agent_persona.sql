-- Schema definition for the agent_persona table
-- Maps a simulation user (agent) to a persona.
CREATE TABLE IF NOT EXISTS agent_persona (
    user_id INTEGER PRIMARY KEY,
    persona_id TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES user(user_id),
    FOREIGN KEY(persona_id) REFERENCES persona(persona_id)
);
