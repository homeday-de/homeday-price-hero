REFLECT_AVIVID = """
            SELECT aviv_geo_id FROM geo_cache 
            WHERE geo_index IN (%s)
        """

VALIDATE_PRICE_GEN = """
            SELECT aviv_geo_id FROM geo_cache
            WHERE aviv_geo_id != 'no_aviv_id_available'
            AND aviv_geo_id NOT IN (
                SELECT aviv_geo_id FROM prices_all 
                WHERE price_date = {} AND transaction_type IS NOT NULL
            )
        """

GET_SEQUENCE_VALUE = "SELECT last_value FROM {}"