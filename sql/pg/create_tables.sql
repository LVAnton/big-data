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
)