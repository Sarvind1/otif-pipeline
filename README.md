# OTIF Pipeline

A Python ETL system for calculating On-Time In-Full (OTIF) metrics on purchase order data. Ingests data from Redshift and Excel sources, calculates delivery performance metrics including Turnaround Time (TAT), and exports results to SharePoint and Excel.

## Key Features

- **Multi-source data ingestion**: Redshift SQL queries and Excel/CSV file processing with concurrent fetching
- **TAT calculation**: Configurable stage-based turnaround time computation with dependency tracking
- **OTIF metrics**: Delivery performance analysis across multiple supply chain stages
- **Flexible configuration**: YAML-based stage configuration supporting dynamic expressions and fallback calculations
- **SharePoint integration**: Automated upload of analysis results and reports
- **Scalable processing**: Multi-threaded data fetching and batch processing

## Tech Stack

- **Python 3**: Core language
- **pandas**: Data manipulation and analysis
- **Redshift**: Primary data warehouse connection
- **Pydantic**: Configuration validation
- **SharePoint API**: Result publishing
- **Jupyter**: Interactive analysis and development

## Setup

### Prerequisites
- Python 3.8+
- Access to Redshift database
- SharePoint credentials (for publishing)
- Excel files with mapping tables (`local_data_dnd/`)

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/otif-pipeline.git
cd otif-pipeline

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configuration

1. **Set up credentials** - Create a `creds.txt` file with your database credentials:
   ```
   redshift_user=your_username
   redshift_password=your_password
   redshift_host=your_redshift_host
   redshift_port=5439
   ```

2. **Configure stages** - Update stage configuration in `models_config.py` to define OTIF calculation rules

3. **Map local data** - Ensure all required mapping files are present in `local_data_dnd/`

## Usage

### Run the Complete Pipeline

```bash
python app.py
```

This executes the full ETL workflow:
1. Load credentials and configuration
2. Ingest data tables from Redshift
3. Ingest mapping Excel files
4. Calculate final OTIF dataframe
5. Calculate Day-over-Day (DoD) view
6. Upload results to SharePoint

### Run Individual Components

```python
from ingestion_tables_multithreading import main as ingest_tables
from main import main as calculate_otif

# Ingest only tables
dfs_tables = ingest_tables(creds)

# Calculate OTIF metrics
final_df = calculate_otif(dfs_tables, dfs_excels)
```

### TAT Calculation

The TAT calculator uses configurable stage definitions:

```python
from stage_calculator_0829 import StageCalculator

calculator = StageCalculator(config, expression_evaluator)
adjusted_timestamp, reasoning = calculator.calculate_adjusted_timestamp(stage_id, po_row)
```

## Output

Results are generated in the `outputs/` directory:
- `csv_files/`: Processed dataframes as CSV
- `excel_exports/`: Stage-level analysis Excel files
- `tat_results/`: Detailed TAT calculations as JSON
- `logs/`: Processing logs and debug information

All outputs are automatically uploaded to SharePoint upon completion.

## Project Structure

```
├── app.py                              # Main entry point
├── main.py                             # OTIF calculation orchestration
├── ingestion_*.py                      # Data ingestion modules
├── stage_calculator_*.py                # TAT calculation engines
├── dod.py                              # Day-over-Day analysis
├── models_config.py                    # Configuration models
├── local_data_dnd/                     # Static mapping data
│   ├── excels/                         # Excel-based mappings
│   └── tables/                         # Static data tables
└── outputs/                            # Generated results (not tracked)
```

## Notes

- Credentials should be managed via environment variables or secure vaults for production use
- Large data files and outputs are excluded from version control
- TAT calculation supports dynamic expressions and fallback logic
- Processing logs are available in `outputs/logs/` for debugging