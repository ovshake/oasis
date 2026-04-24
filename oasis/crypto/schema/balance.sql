-- Schema definition for the balance table
CREATE TABLE IF NOT EXISTS balance (
    user_id INTEGER NOT NULL,
    instrument_id INTEGER NOT NULL,
    amount REAL NOT NULL DEFAULT 0,
    locked REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(user_id, instrument_id),
    FOREIGN KEY(user_id) REFERENCES user(user_id),
    FOREIGN KEY(instrument_id) REFERENCES instrument(instrument_id)
);
