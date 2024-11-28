create_prices_all = """
    CREATE TABLE IF NOT EXISTS prices_all (
        geo_id TEXT,
        price_date TEXT,
        transaction_type TEXT,
        house_price JSON,
        apartment_price JSON,
        hybrid_price JSON,
        PRIMARY KEY (geo_id, price_date)
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
    ON CONFLICT (geo_id, price_date) DO NOTHING
"""

create_report_batches = """
    CREATE TABLE IF NOT EXISTS report_batches (
        id bigserial NOT NULL,
        name varchar NULL,
        started_at timestamp NULL,
        completed_at timestamp NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NOT NULL,
        CONSTRAINT report_batches_pkey PRIMARY KEY (id)
    )
"""

create_report_headers = """
    CREATE TABLE IF NOT EXISTS report_headers (
        id uuid DEFAULT uuid_generate_v4() NOT NULL,
        "name" varchar NULL,
        marketing_type varchar NULL,
        completed_at timestamp NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NOT NULL,
        property_type varchar NULL,
        city varchar NULL,
        country varchar NULL,
        "date" date NOT NULL,
        active bool DEFAULT false NULL,
        "source" int4 DEFAULT 0 NULL,
        report_batch_id int8 NULL,
        CONSTRAINT report_headers_pkey PRIMARY KEY (id)
    )
"""

create_location_prices = """
    CREATE TABLE IF NOT EXISTS location_prices (
        id uuid DEFAULT uuid_generate_v4() NOT NULL,
        report_header_id uuid NULL,
        city varchar NULL,
        district varchar NULL,
        zip_code varchar NULL,
        price numeric(15, 2) NULL,
        unit varchar NULL,
        min numeric(15, 2) NULL,
        max numeric(15, 2) NULL,
        mean numeric(15, 2) NULL,
        median numeric(15, 2) NULL,
        standard_deviation numeric NULL,
        "interval" daterange NULL,
        created_at timestamp NOT NULL,
        updated_at timestamp NOT NULL,
        country varchar NULL,
        country_id varchar NULL,
        city_id varchar NULL,
        district_id varchar NULL,
        zip_code_id varchar NULL,
        score numeric NULL,
        CONSTRAINT location_prices_pkey PRIMARY KEY (id)
    )
"""

create_ = {'geo_cache': create_geo_cache, 'prices_all': create_prices_all, 'prices_mapped': [create_report_batches, create_report_headers, create_location_prices]}
insert_ = {'geo_cache': insert_geo_cache, 'prices_all': insert_prices_all}
