create_prices_all = """
    CREATE TABLE IF NOT EXISTS prices_all (
        aviv_geo_id TEXT,
        price_date TEXT,
        transaction_type TEXT,
        house_price JSON,
        apartment_price JSON,
        hybrid_price JSON,
        PRIMARY KEY (aviv_geo_id, price_date)
    )
"""

create_geo_cache = """
    CREATE TABLE IF NOT EXISTS geo_cache (
        geo_index TEXT PRIMARY KEY,
        aviv_geo_id TEXT,
        type_key TEXT,
        coordinates JSON,
        match_name TEXT,
        confidence_score INT
    )
"""

insert_geo_cache = """
    INSERT INTO geo_cache (
        geo_index, aviv_geo_id, type_key, coordinates, match_name, confidence_score
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (geo_index) DO NOTHING
"""

insert_prices_all = """
    INSERT INTO prices_all (
        aviv_geo_id, price_date, transaction_type, house_price, apartment_price, hybrid_price
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (aviv_geo_id, price_date) DO NOTHING
"""


create_ = {'geo_cache': create_geo_cache, 'prices_all': create_prices_all}
insert_ = {'geo_cache': insert_geo_cache, 'prices_all': insert_prices_all}
