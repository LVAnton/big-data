CREATE TABLE IF NOT EXISTS iphone_ads(
    id SERIAL PRIMARY KEY,
    title TEXT,
    city TEXT,
    url TEXT UNIQUE,
    price INT,
    img_key TEXT,
    location TEXT,
    is_new BOOL,
    seller_score FLOAT,
    seller_rates INT,
    description TEXT
);

CREATE TABLE IF NOT EXISTS hh_vacancies (
    id SERIAL PRIMARY KEY,
    vacancy_id TEXT UNIQUE,
    title TEXT,
    employer TEXT,
    city TEXT,
    salary_from INT,
    salary_to INT,
    url TEXT,
    published_at TIMESTAMP
);