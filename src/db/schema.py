create_prices_all = """
    CREATE TABLE IF NOT EXISTS prices_all (
        id SERIAL PRIMARY KEY,
        geo_id TEXT,
        price_date TEXT,
        transaction_type TEXT,
        house_price JSON,
        apartment_price JSON,
        hybrid_price JSON
    )
"""

create_geo_cache = """
    CREATE TABLE IF NOT EXISTS geo_cache (
        zip_code TEXT PRIMARY KEY,
        geo_id TEXT,
        type_key TEXT,
        coordinates JSON,
        match_name TEXT,
        confidence_score INT
    )
"""

insert_geo_cache = """
    INSERT INTO geo_cache (
        zip_code, geo_id, type_key, coordinates, match_name, confidence_score
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (zip_code) DO NOTHING
"""

insert_prices_all = """
    INSERT INTO prices_all (
        geo_id, price_date, transaction_type, house_price, apartment_price, hybrid_price
    ) VALUES (%s, %s, %s, %s, %s, %s)
"""


create_ = {'geo_cache': create_geo_cache, 'prices_all': create_prices_all}
insert_ = {'geo_cache': insert_geo_cache, 'prices_all': insert_prices_all}
