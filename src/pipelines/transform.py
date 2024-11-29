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


class TransformAVIVRawToHDPrices:
    pass
