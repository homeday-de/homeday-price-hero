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
    ON CONFLICT (aviv_geo_id, price_date)
    DO UPDATE SET
        transaction_type = COALESCE(EXCLUDED.transaction_type, prices_all.transaction_type),
        house_price = EXCLUDED.house_price,
        apartment_price = EXCLUDED.apartment_price,
        hybrid_price = EXCLUDED.hybrid_price
    WHERE prices_all.transaction_type IS NULL
"""

insert_source = {
    'geo_cache': insert_geo_cache, 
    'prices_all': insert_prices_all
}


SQL_REPORT_BATCHES = """
        INSERT INTO report_batches (name, started_at, completed_at, created_at, updated_at)
        SELECT
            CONCAT(
                'AVIV-', EXTRACT(YEAR FROM price_date::date)::TEXT, 'Q', EXTRACT(QUARTER FROM price_date::date)::TEXT
            ) AS name,
            CURRENT_TIMESTAMP AS started_at,
            CURRENT_TIMESTAMP AS completed_at,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM (
            SELECT DISTINCT price_date
            FROM prices_all
        ) price_dates
        ON CONFLICT (name) DO NOTHING
    """

SQL_REPORT_HEADERS = """
    -- Update existing rows with the same report_batch_id to set active = FALSE
    WITH distinct_prices AS (
        -- Extract distinct price_date and map to year-quarter format
        SELECT DISTINCT 
            DATE_TRUNC('quarter', price_date::date)::date AS price_date,
            CONCAT(
                'AVIV-', EXTRACT(YEAR FROM price_date::date)::TEXT, 'Q', EXTRACT(QUARTER FROM price_date::date)::TEXT
            ) AS quarter_name
        FROM prices_all
    ),
    batch_mapping AS (
        -- Join distinct_prices with report_batches to get report_batch_id for each quarter
        SELECT 
            dp.price_date,
            rb.id AS report_batch_id
        FROM distinct_prices dp
        JOIN report_batches rb
        ON dp.quarter_name = rb.name
    ),
    expanded_rows AS (
        -- Generate the four rows per report_batch_id
        SELECT 
            bm.report_batch_id,
            bm.price_date,
            params.name,
            params.property_type
        FROM batch_mapping bm
        CROSS JOIN (
            VALUES 
                ('zip_codes', 'apartment'),
                ('zip_codes', 'house'),
                ('cities', 'apartment'),
                ('cities', 'house')
            ) AS params(name, property_type)
    ),
    update_existing AS (
        -- Update existing rows with the same report_batch_id to set active = FALSE
        UPDATE report_headers
        SET active = FALSE
        WHERE report_batch_id IN (
            SELECT DISTINCT report_batch_id
            FROM expanded_rows
        ) AND active = TRUE
        RETURNING report_batch_id
    )
    -- Insert new rows
    INSERT INTO report_headers (
        name, property_type, marketing_type, completed_at, created_at, 
        updated_at, city, country, date, active, source, report_batch_id
    )
    SELECT 
        er.name,
        er.property_type,
        'sell' AS marketing_type,
        CURRENT_TIMESTAMP AS completed_at,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at,
        NULL AS city,
        'DE' AS country, -- Static value, adjust as needed
        er.price_date AS date, -- Use price_date from batch_mapping
        TRUE AS active,
        1 AS source, -- Default value, adjust as needed
        er.report_batch_id
    FROM expanded_rows er
    ON CONFLICT (id) DO NOTHING;
"""

SQL_LOCATION_PRICES = """
    WITH price_data AS (
        -- Extract prices based on property_type from prices_all
        SELECT 
            pa.aviv_geo_id,
            DATE_TRUNC('quarter', price_date::date)::date AS price_date,
            daterange(
                (price_date::date - make_interval(months => 3))::date, price_date::date
            ) AS interval,
            CASE
                WHEN rh.property_type = 'apartment' THEN 
                    COALESCE(
                        (pa.apartment_price->>'value')::numeric, 
                        (pa.hybrid_price->>'value')::numeric
                    )
                WHEN rh.property_type = 'house' THEN 
                    COALESCE(
                        (pa.house_price->>'value')::numeric, 
                        (pa.hybrid_price->>'value')::numeric
                    )
            END AS price,
            CASE
                WHEN rh.property_type = 'apartment' THEN 
                    COALESCE(
                        (pa.apartment_price->>'high')::numeric, 
                        (pa.hybrid_price->>'high')::numeric
                    )
                WHEN rh.property_type = 'house' THEN 
                    COALESCE(
                        (pa.house_price->>'high')::numeric, 
                        (pa.hybrid_price->>'high')::numeric
                    )
            END AS max,
            CASE
                WHEN rh.property_type = 'apartment' THEN 
                    COALESCE(
                        (pa.apartment_price->>'low')::numeric, 
                        (pa.hybrid_price->>'low')::numeric
                    )
                WHEN rh.property_type = 'house' THEN 
                    COALESCE(
                        (pa.house_price->>'low')::numeric, 
                        (pa.hybrid_price->>'low')::numeric
                    )
            END AS min,
            CASE
                WHEN rh.property_type = 'apartment' THEN 
                    COALESCE(
                        (pa.apartment_price->>'accuracy')::numeric, 
                        (pa.hybrid_price->>'accuracy')::numeric
                    )
                WHEN rh.property_type = 'house' THEN 
                    COALESCE(
                        (pa.house_price->>'accuracy')::numeric, 
                        (pa.hybrid_price->>'accuracy')::numeric
                    )
            END AS score,
            rh.id AS report_header_id,
            rh.name AS header_name
        FROM prices_all pa
        JOIN report_headers rh
        ON pa.price_date::date = rh.date
        WHERE rh.active = TRUE
    ),
    geo_data AS (
        -- Map geo_cache information for zip_code and city_id
        SELECT
            gc.geo_index,
            gc.aviv_geo_id,
            gc.hd_geo_id,
            CASE
                WHEN gc.hd_geo_id = 'no_hd_geo_id_applicable' THEN gc.geo_index
                ELSE NULL
            END AS zip_code,
            CASE
                WHEN gc.hd_geo_id != 'no_hd_geo_id_applicable' THEN gc.hd_geo_id
                ELSE NULL
            END AS city_id
        FROM geo_cache gc
    )
    INSERT INTO location_prices (
        report_header_id, city_id, zip_code, price, unit, median, 
        created_at, updated_at, country, max, min, score, interval
    )
    SELECT 
        pd.report_header_id,
        gd.city_id,
        gd.zip_code,
        pd.price,
        'EUR_SQM' AS unit,
        pd.price AS median,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at,
        'DE' AS country,
        pd.max,
        pd.min,
        pd.score,
        pd.interval
    FROM price_data pd
    inner JOIN geo_data gd
    ON pd.aviv_geo_id = gd.aviv_geo_id
    WHERE 
        -- Map `zip_codes` to `hd_geo_id = 'no_hd_geo_id_applicable'`
        (pd.header_name = 'zip_codes' AND gd.hd_geo_id = 'no_hd_geo_id_applicable')
        OR
        -- Map `cities` to valid `hd_geo_id`
        (pd.header_name = 'cities' AND gd.hd_geo_id != 'no_hd_geo_id_applicable')
    ON CONFLICT (id) DO NOTHING
"""

insert_price_map = {
    "report_batches": SQL_REPORT_BATCHES,
    "report_headers": SQL_REPORT_HEADERS,
    "location_prices": SQL_LOCATION_PRICES
}