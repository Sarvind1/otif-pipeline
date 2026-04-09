# OTIF Automation Pipeline

An automated supply chain management system that calculates On-Time In-Full (OTIF) metrics and Turn-Around Time (TAT) for purchase order processing. The pipeline ingests data from Redshift and Excel sources, performs complex multi-stage calculations with dependency tracking, and exports results to Excel and SharePoint.

## Key Features

- **Multi-threaded Data Ingestion**: Efficiently fetch purchase order and supplier data from Redshift using concurrent connections
- **Reference Data Management**: Load and map static/dynamic reference data from Excel files (vendor mappings, payment terms, compliance data, etc.)
- **Stage-based TAT Calculation**: Calculate adjusted timestamps through configurable supply chain stages (PO creation, receipt, customs clearance, warehouse inbound, etc.) with dependency-driven logic
- **Flexible Expression Evaluation**: Support dynamic date expressions and stage dependencies for complex business rules
- **Day-of-Day (DoD) Analysis**: Generate day-by-day performance metrics and delay analysis
- **Batch Processing & Exports**: Generate detailed Excel and JSON output files with stage-level analysis
- **SharePoint Integration**: Automatically upload results and logs to SharePoint for team visibility

## Tech Stack

- **Python 3.x**: Core automation language
- **pandas**: Data manipulation and analysis
- **Redshift**: Data warehouse for PO and supplier data
- **openpyxl / xlrd**: Excel file handling
- **SharePoint API**: Result publishing
- **Pydantic**: Configuration validation and data models
- **Jupyter Notebooks**: Development and testing

## Setup

### Prerequisites
- Python 3.7+
- Access to Redshift database with purchase order and supplier data
- SharePoint credentials (for upload step)
- Local reference data files in `local_data_dnd/`

### Installation

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # macOS/Linux
   # or
   .venv\Scripts\activate  # Windows
   ```

2. Install dependencies:
   ```bash
   pip install pandas openpyxl pydantic requests sharepoint
   ```

3. Configure credentials in `creds.txt`:
   ```
   redshift_user=<user>
   redshift_password=<password>
   redshift_host=<host>
   redshift_port=5439
   redshift_database=<database>
   sharepoint_user=<user>
   sharepoint_password=<password>
   ```

### Running the Pipeline

```bash
python app.py
```

The script will:
1. Load credentials from `creds.txt`
2. Ingest tables from Redshift (multi-threaded)
3. Load reference data from Excel files
4. Calculate OTIF and TAT metrics
5. Generate Day-of-Day analysis
6. Export results to `outputs/` directory
7. Upload to SharePoint

## Output Files

- **CSV Exports**: `outputs/csv_files/processed_data_*.csv` — Stage-level detail data
- **Excel Reports**: `outputs/excel_exports/stage_level_analysis_*.xlsx` — Formatted stage analysis
- **TAT Results**: `outputs/tat_results/tat_results_*.json` — Complete calculation results with reasoning
- **Logs**: `outputs/logs/tat_calculation.log` — Detailed pipeline execution logs

## Configuration

TAT stages and calculation rules are defined in stage configuration files. Each stage supports:
- Projected/Actual/Adjusted calculation methods
- Configurable lead times
- Dynamic expression-based calculations
- Dependency on preceding stages

Modify stage configs to adjust calculation logic for your business rules.

## Project Structure

```
├── main.py                    # Core calculation orchestrator
├── app.py                     # Pipeline entry point with SharePoint integration
├── ingestion_tables_multithreading.py  # Redshift data fetch
├── ingestion_excels.py        # Excel reference data loading
├── tat_calculator.py          # TAT calculation engine
├── stage_calculator_*.py      # Individual stage calculation logic
├── dod.py                     # Day-of-Day analysis
├── expression_evaluator.py    # Dynamic expression evaluation
├── models_config.py           # Pydantic configuration models
├── local_data_dnd/            # Reference data (mappings, lookups)
└── outputs/                   # Generated results and logs
```