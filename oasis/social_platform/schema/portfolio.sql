CREATE TABLE IF NOT EXISTS portfolio (
    user_id INTEGER NOT NULL,
    company_id INTEGER NOT NULL,
    shares INTEGER DEFAULT 0,
    PRIMARY KEY(user_id, company_id),
    FOREIGN KEY(user_id) REFERENCES user(user_id),
    FOREIGN KEY(company_id) REFERENCES company(company_id)
);
