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
        geo_index TEXT,
        hd_geo_id TEXT,
        aviv_geo_id TEXT,
        type_key TEXT,
        coordinates JSON,
        match_name TEXT,
        confidence_score INT,
        PRIMARY KEY (geo_index, hd_geo_id)
    )
"""

insert_geo_cache = """
    INSERT INTO geo_cache (
        geo_index, hd_geo_id, aviv_geo_id, type_key, coordinates, match_name, confidence_score
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (geo_index, hd_geo_id) DO NOTHING
"""

insert_prices_all = """
    INSERT INTO prices_all (
        aviv_geo_id, price_date, transaction_type, house_price, apartment_price, hybrid_price
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (aviv_geo_id, price_date) DO NOTHING
"""

create_report_batches = {
    "report_batches": """
        CREATE TABLE IF NOT EXISTS report_batches (
            id bigserial NOT NULL,
            name varchar NULL,
            started_at timestamp NULL,
            completed_at timestamp NULL,
            created_at timestamp NOT NULL,
            updated_at timestamp NOT NULL,
            CONSTRAINT report_batches_pkey PRIMARY KEY (id)
        )
    """,
    "index_created_at": "CREATE INDEX IF NOT EXISTS index_report_batches_on_created_at ON report_batches USING btree (created_at);",
    "index_name": "CREATE UNIQUE INDEX IF NOT EXISTS index_report_batches_on_name ON report_batches USING btree (name);"
}

create_report_headers = {
    "report_headers": """
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
    """,
    "index_created_at": "CREATE INDEX IF NOT EXISTS index_report_headers_on_created_at ON report_headers USING btree (created_at);",
    "index_name": "CREATE INDEX IF NOT EXISTS index_report_headers_on_name ON report_headers USING btree (name);",
    "index_report_batch_id": "CREATE INDEX IF NOT EXISTS index_report_headers_on_report_batch_id ON report_headers USING btree (report_batch_id);",
    "index_type_and_location_fields": "CREATE INDEX IF NOT EXISTS index_report_headers_on_type_and_location_fields ON report_headers USING btree (active, name, country, city, marketing_type, date);"
}

create_location_prices = {
    "location_prices": """
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
    """,
    "index_city_id": "CREATE INDEX IF NOT EXISTS index_location_prices_on_city_id ON location_prices USING btree (city_id);",
    "index_country": "CREATE INDEX IF NOT EXISTS index_location_prices_on_country ON location_prices USING btree (country);",
    "index_country_and_city_and_district": "CREATE INDEX IF NOT EXISTS index_location_prices_on_country_and_city_and_district ON location_prices USING btree (country, city, district);",
    "index_report_header_id": "CREATE INDEX IF NOT EXISTS index_location_prices_on_report_header_id ON location_prices USING btree (report_header_id);"
}

extensions = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'

create_ = {'geo_cache': create_geo_cache, 'prices_all': create_prices_all, 'extensions': extensions}
insert_ = {'geo_cache': insert_geo_cache, 'prices_all': insert_prices_all}
create_price_map_schema = {"report_batches": create_report_batches, "report_headers": create_report_headers, "location_prices": create_location_prices}
