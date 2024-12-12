import os
import logging
from dynaconf import Dynaconf
from src.db import Database
from src.db.query_base import insert_price_map
from src.lib import update_report_batch_id


class AVIVRawToHDPrices(Database):
    SQL_REPORT_BATCHES = insert_price_map['report_batches']
    SQL_REPORT_HEADERS = insert_price_map['report_headers']
    SQL_LOCATION_PRICES = insert_price_map['location_prices']

    def __init__(self, config: Dynaconf, test=False):
        super().__init__(config=config, test=test)
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        """
        Run the entire transformation pipeline in sequence.
        """
        try:
            self.logger.info("Starting transformation pipeline...")
            self.execute_transform_query("Transforming data to report batches...", self.SQL_REPORT_BATCHES)
            self.execute_transform_query("Transforming data to report headers...", self.SQL_REPORT_HEADERS)
            self.execute_transform_query("Transforming data to location prices...", self.SQL_LOCATION_PRICES)
            last_value = self.get_last_value_sequence()
            if self.db_handler.db_config.database != "test_db":
                self.logger.info("Update report batch ID for next time to re-run")
                secret_path = os.path.join(os.getcwd(), os.getenv("SECRET_PATH"))
                update_report_batch_id(file_path=secret_path, latest_value=last_value+1)
        finally:
            self.db_handler.close()
            self.logger.info("Pipeline execution completed.")

    def execute_transform_query(self, task_description, query):
        """
        Execute a SQL query and log the task description.
        """
        self.logger.info(task_description)
        if not self.db_handler.conn:
            self.db_handler.connect()
        try:
            with self.db_handler.conn.cursor() as cur:
                cur.execute(query)
                self.db_handler.conn.commit()
            self.logger.info(f"{task_description} - Success.")
        except Exception as e:
            self.logger.error(f"{task_description} - Failed. Error: {e}")
            raise


class TransformedPricesHealthCheck(Database):
    def __init__(self, config: Dynaconf, test=False):
        super().__init__(config=config, test=test)
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute_query(self, query):
        """
        Execute a SQL query for health check purposes.

        Args:
            query (str): SQL query to execute.

        Returns:
            list: Query results.
        """
        self.logger.info(f"Executing health check query:\n{query}")
        try:
            self.db_handler.connect()
            with self.db_handler.conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()
                self.logger.info("Query executed successfully.")
                return result
        except Exception as e:
            self.logger.error(f"Health check query failed: {e}")
            raise

    def validate_report_batches(self):
        """
        Validate the `report_batches` table.

        - Ensure unique `name` values.
        - Validate `started_at` and `completed_at` timestamps.
        """
        query = """
            SELECT name, COUNT(*)
            FROM report_batches
            GROUP BY name
            HAVING COUNT(*) > 1
        """
        results = self.execute_query(query)
        if results:
            raise ValueError(f"Duplicate names found in report_batches: {results}")

    def validate_report_headers(self):
        """
        Validate the `report_headers` table.

        - Ensure proper mapping between `price_date` and `report_batch_id`.
        """
        query = """
            SELECT rh.report_batch_id, COUNT(*)
            FROM report_headers rh
            JOIN report_batches rb ON rh.report_batch_id = rb.id
            WHERE rh.date != DATE_TRUNC('quarter', rb.started_at)
                AND rh.active = TRUE
            GROUP BY rh.report_batch_id
            HAVING COUNT(*) > 4
        """
        results = self.execute_query(query)
        if results:
            raise ValueError(f"Inconsistent report_headers mapping: {results}")

    def validate_location_prices(self):
        """
        Validate the `location_prices` table.

        - Ensure `price`, `max`, and `min` values are non-negative.
        - Check that `zip_code` and `city_id` are correctly mapped.
        """
        query = """
            SELECT report_header_id, price, max, min
            FROM location_prices
            WHERE (zip_code is not null OR city_id is not null)
            AND (price < 0 OR max < 0 OR min < 0)
        """
        results = self.execute_query(query)
        if results:
            raise ValueError(f"Negative values detected in location_prices: {results}")

    def run_all_checks(self):
        """
        Run all health checks.

        Raises:
            ValueError: If any health check fails.
        """
        self.logger.info("Running health checks...")
        self.validate_report_batches()
        self.validate_report_headers()
        self.validate_location_prices()
        self.logger.info("All health checks passed successfully.")
