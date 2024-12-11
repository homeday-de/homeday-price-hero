# Homeday Price Hero

**Homeday Prices Hero** is a service powering Homeday's Preisatlas by generating quarterly price estimates. It integrates data from the **GeocodingAPI** ([AVIV Geo Services](https://avivgroup.atlassian.net/wiki/spaces/AGRS/pages/204505110/AVIV+Geo+Services)) and **PriceAPI** ([AVIV Market Insights Services](https://avivgroup.atlassian.net/wiki/spaces/AGRS/pages/490111012/AVIV+Market+Insights+Services)) and transforms it for use in the Homeday Prices Database.

## Features

- **Data Pipeline**: Extract, cache, validate, and transform data into the Homeday Prices Database schema.
- **Cloud Backup**: Backup source data to cloud storage.
- **Database Synchronization**: Sync data with the Homeday Prices Database on RDS.
- **Quarterly Processing**: (Manual) Ensures up-to-date and accurate price estimates.

## Getting Started (For CLI Users)

### Prerequisites

- AVIV-provided machine with:
  - **AVIV VPN** (via Cloudflare)
  - **Homeday VPN** setup
  - AWS CLI with default profile or a configured profile:
    First, ensure that your Homeday AWS IAM users have permission to read and write secrets in AWS SecretManager.
    ```bash
    aws configure # Default profile
    aws configure --profile <your-profile-name> # Separate profile
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

1. **Run ETL Pipeline**:
   ```bash
   docker-compose exec price_hero python cli.py
   ```
   - Type `etl`, year, and quarter at the prompts. Ensure AVIV VPN (Cloudflare) is enabled:
     ```bash
     Which process is going to continue? (etl, sync): etl
     Enter a valid year (e.g., 2024): 2024
     Enter a quarter (e.g., Q1, Q2, Q3, Q4): Q4
     ```

2. **Sync Data with RDS**:
   - Disconnect from Cloudflare VPN.
   - Connect to Homeday VPN.
   - Run synchronization:
     ```bash
     Which process is going to continue? (etl, sync): sync
     ```

3. **Clean Data**:
   Remove containers and volumes when finished:
   ```bash
   docker-compose down -v --rmi all --remove-orphans
   ```

## Contributing

1. Install Python >= 3.10 and dependencies in separated virtual enviroment:
   ```bash
   python3 -m venv .venv            # Set up venv
   source .venv/bin/activate        # Activate venv
   pip install -r requirements.txt  # Install dependencies
   ```
2. Set up the configuration:
   ```bash
   python detect_config.py --get
   ```
3. After devlopment and run tests:
   ```bash
   pytest tests/
   ```
4. Submit a pull request.

## Troubleshooting

1. **ETL Availability**:
   - APIs (dev and preview environments) are accessible between **6 AM UTC and 7 PM UTC, Monday to Friday**.
   - Network issues may result in **504 errors**. Re-execution is safe, as the caching feature prevents redundant requests.
