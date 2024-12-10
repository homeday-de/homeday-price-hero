import asyncio
import asyncclick as click
import logging
from src.db import DatabaseHandler
from src.pipelines import APIToPostgres, PostgresToS3, AVIVRawToHDPrices
from src.lib.aws import S3Connector, SecretManager
from src.lib import get_first_day_of_quarter, validate_year


# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def configure_secrets(secret_manager: SecretManager, action: str):
    """
    Handle secrets configuration.
    """
    if action == "get":
        secret_manager.create_config_file('config')
    elif action == "update":
        secret_manager.update_secret_to_vault('config')
    else:
        raise ValueError("Invalid action for configure_secrets.")

async def extract_prices(config, price_date: str, is_test: bool):
    """
    Extract price data from AVIV API and store in PostgreSQL.
    """
    pipeline = APIToPostgres(config, is_test)
    pipeline.initiate_db()
    geo_indices = config.test_geo_indices if is_test else config.geo_indices
    await pipeline.run(geo_indices=geo_indices, price_date=price_date)

def transform_prices(config, is_test: bool):
    """
    Transform raw data to HD prices schema.
    """
    transformer = AVIVRawToHDPrices(config, is_test)
    transformer.run()

def backup_pg_to_filesystem(config, is_test: bool, save_local: bool):
    """
    Backup source tables' data (geo_cache, prices_all) from PostgreSQL to S3 or local data/ folder.
    """
    s3_connector = S3Connector(config)
    loader = PostgresToS3(config, s3_connector=s3_connector, test=is_test)
    for table_name in ['geo_cache', 'prices_all']:
        loader.run(table_name=table_name, local=save_local)


class PricesUpdater:
    def __init__(self, local_config, rds_config, chunk_size=10000):
        self.local_handler = DatabaseHandler(local_config)
        self.rds_handler = DatabaseHandler(rds_config)
        self.chunk_size = chunk_size

    def update_table(self, table_name):
        """Update data from a local table to an RDS table."""
        print(f"Processing table: {table_name}")
        column_names = self.local_handler.fetch_column_names(table_name)
        total_rows = self.local_handler.count_rows(table_name)
        print(f"Total rows in {table_name}: {total_rows}")

        offset = 0
        while offset < total_rows:
            chunk = self.local_handler.fetch_chunked_data(table_name, offset, self.chunk_size)
            self.rds_handler.append_data(table_name, chunk, column_names)

            offset += len(chunk)
            progress = (offset / total_rows) * 100
            print(f"Progress for {table_name}: {progress:.2f}%")

        print(f"Data from {table_name} appended successfully!")

    def run(self, tables):
        """Run the update process for multiple tables."""
        try:
            self.local_handler.connect()
            self.rds_handler.connect()

            for table in tables:
                self.update_table(table)

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            self.local_handler.close()
            self.rds_handler.close()


async def run_etl_process(
    process: str, 
    price_year: str, 
    price_quarter: str, 
    should_transform: bool, 
    is_test: bool, 
    save_local: bool
):
    """
    Execute the ETL process based on the provided parameters.
    """
    secret_manager = SecretManager()
    configure_secrets(secret_manager, action="get")
    from config import settings

    if process == "pricegen":
        click.echo("Run etl to fetch price from aviv and transform to hd prices schema")
        if not price_year or not validate_year(None, None, price_year):
            price_year = click.prompt('Enter a valid year (e.g., 2024):')

        if not price_quarter:
            price_quarter = click.prompt(
                'Enter a quarter (e.g., Q1):', 
                type=click.Choice(["Q1", "Q2", "Q3", "Q4"])
            )

        price_date = get_first_day_of_quarter(price_year + price_quarter)
        await extract_prices(config=settings, price_date=price_date, is_test=is_test)

        if should_transform:
            transform_prices(config=settings, is_test=is_test)

        backup_pg_to_filesystem(config=settings, is_test=is_test, save_local=save_local)
        configure_secrets(secret_manager, action="update")
    else:
        click.echo("Upload transformed tables to hd prices db")
        local_conf = settings.db.dev if not is_test else settings.db.test
        rds_conf = settings.aws.rds_config.prices_staging
        price_updater = PricesUpdater(local_conf, rds_conf)
        tables = [
            "report_batches"
            "report_headers", 
            "location_prices"
        ]
        price_updater.run(tables)


@click.command()
@click.option(
    "--process",
    prompt="Which process is going to continue?",
    type=click.Choice(["pricegen", "upload2hdrds"]), 
    required=True,
    help="Specify the process to execute."
)
@click.option('--price_year', help='Year for AVIV price API query.')
@click.option('--price_quarter', type=click.Choice(["Q1", "Q2", "Q3", "Q4"]), help='Quarter for AVIV price API query.')
@click.option('--transform', is_flag=True, help='Run data transformation to HD prices schema.')
@click.option('--test', is_flag=True, default=True, help='Run in test mode.')
@click.option('--local', is_flag=True, default=True, help='Save source data tables locally.')
async def main(process, price_year, price_quarter, transform, test, local):
    """
    Entry point for the ETL script.
    """
    await run_etl_process(
        process=process,
        price_year=price_year,
        price_quarter=price_quarter,
        should_transform=transform,
        is_test=test,
        save_local=local
    )

if __name__ == "__main__":
    main(_anyio_backend="asyncio")
