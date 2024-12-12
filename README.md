# Homeday Price Hero

**Homeday Prices Hero** is a service powering Homeday's Preisatlas by generating quarterly price estimates. It integrates data from the **GeocodingAPI** ([AVIV Geo Services](https://avivgroup.atlassian.net/wiki/spaces/AGRS/pages/204505110/AVIV+Geo+Services)) and **PriceAPI** ([AVIV Market Insights Services](https://avivgroup.atlassian.net/wiki/spaces/AGRS/pages/490111012/AVIV+Market+Insights+Services)) and transforms it for use in the Homeday Prices Database.

## Features

- **Extraction Pipeline**: Fetch, extract, cache, validate data from AVIV APIs into development database which has the same schema with Homeday Prices Database.
- **Cloud/On-prem Backup**: Backup source data to S3 or on-prem.
- **Database Synchronization**: Perform data transformation and sync data with the Homeday Prices Database on RDS.

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
2. Enable execute to the script
   ```bash
   chmod +x price_hero.sh
   ```
3. Start the services:
   ```bash
   ./price_hero start
   ```
4. To get more info of the command line tool
   ```bash
   ./price_hero help
   ```

### Usage

Before start, ensure **AVIV VPN (Cloudflare)** is enabled.

1. **If you prefer to run in test mode, add `--test` flag**:
   ```bash
   ./price_hero run --test
   ```
   **Else**:
   ```bash
   ./price_hero run
   ```

2. **Run price extraction and ingestion pipeline**:
   - Type `fetch`, year, and quarter at the prompts.
     ```bash
     Which process is going to continue? (fetch, sync): fetch
     Enter a valid year (e.g., 2024): 2024
     Enter a quarter (e.g., Q1, Q2, Q3, Q4): Q4
     ```

3. **Run data transformation and sync data with RDS**:
   - Disconnect from Cloudflare VPN.
   - Connect to Homeday VPN.
   - Run synchronization:
     ```bash
     Which process is going to continue? (fetch, sync): sync
     ```

4. **Clean Data**:
   Remove containers and volumes when finished:
   ```bash
   ./price_hero clean
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

1. **API Availability**:
   - APIs (dev and preview environments) are accessible between **6 AM UTC and 7 PM UTC, Monday to Friday**.
   - Network issues may result in **504 errors**. Re-execution is safe, as the caching feature prevents redundant requests.
2. For the other questions of GeocodingAPI, please contact `#aviv_bureau_of_geographic_affairs` channel on Slack.
3. 
