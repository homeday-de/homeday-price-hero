# Homeday Price Hero

**Homeday Prices Hero** is a service powering Homeday's Preisatlas by generating quarterly price estimates. It integrates data from the **GeocodingAPI** ([AVIV Geo Services](https://avivgroup.atlassian.net/wiki/spaces/AGRS/pages/204505110/AVIV+Geo+Services)) and **PriceAPI** ([AVIV Market Insights Services](https://avivgroup.atlassian.net/wiki/spaces/AGRS/pages/490111012/AVIV+Market+Insights+Services)) and transforms it for use in the Homeday Prices Database.

## Features

- **Data Pipeline**: Extract, cache, validate, and transform data into the Homeday Prices Database schema.
- **Cloud Backup**: Backup source data to cloud storage.
- **Database Synchronization**: Sync data with the Homeday Prices Database on RDS.
- **Quarterly Processing** (should be done manually so far): Ensures up-to-date and accurate price estimates.

## Getting Started (For CLI Users)

### Prerequisites

- AVIV-provided machine with:
  - **AVIV VPN** (via Cloudflare)
  - **Homeday VPN** setup
  - AWS CLI with default profile or a configured profile:
    First, ensure that your Homeday AWS IAM users have permission to read and write secrets in AWS SecretManager.
    ```bash
    # Configure as default profile
    aws configure

    # Configure as separated profile
    aws configure --profile <your-profile-name>
    ```
- Docker and Docker Compose

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/homeday-de/homeday-price-hero.git
   cd homeday-price-hero
   ```
2. Start the services:
   ```bash
   docker-compose up --build -d
   ```

### Usage

1. Kick-off the process to handle the price
    ```bash
    docker-compose exec price_hero python cli.py
    ```
    1. **Run ETL Pipeline**:
    Type `etl`, year, quarter at the prompts to run an ETL pipeline to acquire, validate, and convert data. Ensure AVIV VPN (Cloudflare) is enabled.
    ```bash
    # Prompt 1
    Which process is going to continue? (etl, sync): etl
    ```
    ```bash
    # Prompt 2
    Enter a valid year (e.g., 2024): 2024
    ```
    ```bash
    # Prompt 3
    Enter a quarter (e.g., Q1): (Q1, Q2, Q3, Q4): Q4
    ```

    2. **Sync Data with RDS**:
    Sync transformed data with the Homeday Prices Database on RDS:
    - **Step 1**: Disconnect from Cloudflare VPN.
    - **Step 2**: Connect to Homeday VPN.
    - **Step 3**: Run: Type `sync` at the prompt to run synchronization from dev db to Homeday Prices DB
        ```bash
        # Prompt
        Which process is going to continue? (etl, sync): sync
        ```

2. Clean Data from local instance
    ```bash
    docker-compose down -v --rmi all --remove-orphans
    ```
    This command removes the application level container and db container and their volumes. Make sure you have completed your work before running this command.

## Contributing

### Local Development Setup

1. Install Python >= 3.10 and dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure the development environment using Docker Compose:
   ```bash
   docker-compose up -d
   ```
3. Test your changes locally and submit a pull request.
    ```bash
    pytest tests/
    ```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
