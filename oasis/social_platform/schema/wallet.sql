CREATE TABLE IF NOT EXISTS wallet (
    user_id INTEGER PRIMARY KEY,
    cash REAL DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES user(user_id)
);
