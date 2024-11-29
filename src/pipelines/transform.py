data_transform_to_report_batches = """
    INSERT INTO report_batches (name, started_at, completed_at, created_at, updated_at)
    SELECT
        CONCAT(EXTRACT(YEAR FROM price_date::date)::TEXT, 'Q', EXTRACT(QUARTER FROM price_date::date)::TEXT) AS name,
        CURRENT_TIMESTAMP AS started_at,
        CURRENT_TIMESTAMP AS completed_at,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM (
        SELECT DISTINCT price_date
        FROM prices_all
    ) price_dates
"""

data_transform_to_report_headers = """
    WITH distinct_prices AS (
        -- Extract distinct price_date and map to year-quarter format
        SELECT DISTINCT 
            price_date::date AS price_date,
            CONCAT(EXTRACT(YEAR FROM price_date::date), 'Q', EXTRACT(QUARTER FROM price_date::date)) AS quarter_name
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
    )
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
    FROM expanded_rows er;
"""


class TransformAVIVRawToHDPrices:
    pass
