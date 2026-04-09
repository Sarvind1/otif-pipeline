# OTIF Pipeline

An automated data pipeline for calculating On-Time In-Full (OTIF) delivery metrics and purchase order turnaround time (TAT) analysis. Ingests purchase order data from Redshift, processes through configurable delivery stages, and exports analytics to Excel/CSV and SharePoint.

## Features

- **Multi-source Data Ingestion**: Fetch PO and operational data from Redshift with concurrent processing
- **Configurable Stage Calculator**: Dynamic TAT calculation for each delivery stage with dependency management
- **Comprehensive Delay Analysis**: Track delays and missed targets across the supply chain workflow
- **Bulk Export**: Generate Excel and CSV reports with stage-level analysis
- **SharePoint Integration**: Automated upload of results to SharePoint for stakeholder access
- **Mapping & Enrichment**: Apply business rules via static lookup tables (payment terms, blockers, compliance data)

## Tech Stack

- **Python 3.x** with pandas, numpy for data processing
- **Redshift** for data warehouse connectivity (via psycopg2)
- **Excel/CSV** file handling (openpyxl, xlrd)
- **SharePoint** client integration
- **Jupyter Notebooks** for exploratory analysis and testing

## Setup

1. **Create virtual environment:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure credentials:**
   Create a `creds.txt` file with Redshift and AWS credentials:
   ```
   user=<redshift_user>
   password=<redshift_password>
   database=<database_name>
   host=<redshift_host>
   port=5439
   AWS_ACCESS_KEY_ID=<aws_key>
   AWS_SECRET_ACCESS_KEY=<aws_secret>
   ```

4. **Place data mappings:**
   Ensure mapping CSVs are in `local_data_dnd/excels/` and table snapshots in `local_data_dnd/tables/`

## Usage

Run the full pipeline:
```bash
python app.py
```

This orchestrates:
1. Load credentials and configuration
2. Ingest tables from Redshift (multithreaded)
3. Load lookup/mapping Excel files
4. Calculate OTIF metrics and TAT delays
5. Generate day-over-day analysis
6. Export results to Excel and SharePoint

Outputs are saved to `outputs/` directory with timestamps.

## Project Structure

- **app.py** - Main entry point and orchestration
- **main.py** - Core metric calculation logic
- **tat_calculator.py** - TAT engine with configurable stages
- **stage_calculator_*.py** - Individual stage delay calculations
- **ingestion_tables_multithreading.py** - Redshift data fetch with threading
- **ingestion_excels.py** - Load mapping/configuration tables
- **dod.py** - Day-over-day analysis
- **local_data_dnd/** - Static lookup tables and reference data
- **documentation/** - Architecture and commit guides