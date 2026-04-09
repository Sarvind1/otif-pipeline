# TAT Calculator System

A comprehensive Python system for calculating Purchase Order (PO) stage timestamps using configurable business logic, replacing complex Excel formulas with maintainable, auditable code.

## Features

- **🗂️ Organized Output Structure**: Clean, segregated folders for all outputs
- **📊 Enhanced Analytics**: Clear, structured insights instead of verbose formulas
- **📤 Excel Export**: Save calculated timestamps alongside original data
- **⚙️ Configuration-Driven**: All business logic externalized in JSON
- **🧮 Dynamic Expression Evaluation**: Support for complex calculations
- **🔗 Dependency Management**: Automatic resolution with cycle detection
- **📈 Comprehensive Reports**: Team workload, critical path, completion rates, delay analysis

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Add your Excel file** (rename to `ts_small.xlsx` or update the filename in `run_tat_calculation.py`)

3. **Run the calculator:**
   ```bash
   python run_tat_calculation.py
   ```

## Organized Output Structure

All outputs are automatically organized into clean folder structure:

```
outputs/
├── tat_results/           # TAT calculation JSON results
│   └── tat_results_20250626_100352.json
├── delay_results/         # Delay analysis JSON results  
│   └── delay_results_20250626_100352.json
├── excel_exports/         # Excel files (exports, reports)
│   ├── tat_export_20250626_100352.xlsx
│   └── delay_report_20250626_100352.xlsx
├── csv_files/            # Processed CSV data files
│   └── processed_data_20250626_100352.csv
└── logs/                 # Application logs and errors
    └── tat_calculation.log
```

## Output Files

- **📋 TAT Results**: Structured calculation results with business insights
- **⏱️ Delay Analysis**: Comprehensive delay reports by stage and team
- **📊 Excel Exports**: Original data + calculated timestamps for all 31 stages
- **📈 Analytics**: Reports on efficiency, bottlenecks, and data quality
- **📄 CSV Files**: Processed data in CSV format for further analysis

## Configuration

All 31 stages are pre-configured in `stages_config.json` with:
- Stage dependencies and lead times
- Team ownership and process types
- Fallback calculations for missing data
- Critical path identification

## Enhanced Output Format

Instead of verbose nested formulas, get clear insights:

```json
{
  "summary": {
    "completion_rate": 90.32,
    "methods_used": {
      "actual_over_precedence": 5,  // Delays detected
      "precedence_over_actual": 3   // Early completions
    }
  },
  "calculation": {
    "method": "actual_over_precedence",
    "decision": "Actual timestamp is later than calculated precedence",
    "actual_date": "2025-06-15T00:00:00",
    "precedence_date": "2025-06-13T00:00:00"
  }
}
```

## Business Intelligence

Automatically identifies:
- 🚨 Critical path delays
- 🚀 Process efficiency gains  
- 📊 Team bottlenecks
- 📈 Data quality issues
- ⏰ Overdue stages requiring attention

## Folder Management

Use the built-in folder manager for maintenance:

```bash
python folder_manager.py
```

Options include:
- Create folder structure
- Clean old files
- Archive results
- Generate structure reports

## Repository Structure

```
tat-calculator/
├── Core Modules/
│   ├── tat_calculator_main.py     # Main coordinator
│   ├── models_config.py           # Data models & config
│   ├── expression_evaluator.py    # Expression engine
│   ├── stage_calculator.py        # Core calculations
│   ├── tat_processor.py          # Batch processing
│   └── delay_calculator.py       # Delay analysis
├── Configuration/
│   └── stages_config.json         # 31-stage configuration
├── Runners/
│   ├── run_tat_calculation.py     # Main runner
│   └── folder_manager.py          # Output management
├── Legacy/
│   └── tat_calculator.py          # Original monolithic version
├── Data/
│   ├── ts_small.xlsx             # Sample data (small)
│   └── ts_big.xlsx               # Full dataset
├── Documentation/
│   ├── project_brain.md          # Architecture guide
│   ├── commits_guide.md          # Change history
│   └── README.md                 # This file
└── outputs/                      # Generated outputs (auto-created)
    ├── tat_results/
    ├── delay_results/
    ├── excel_exports/
    ├── csv_files/
    └── logs/
```

## Architecture

The system uses a **modular architecture** with:
- **Configuration-driven design** - all business logic in JSON
- **Dependency graph (DAG)** - automatic resolution with cycle detection
- **Memoization** - efficient calculations for complex dependencies
- **Expression evaluation** - dynamic, safe parsing using Python AST
- **Organized outputs** - clean folder structure for all generated files

## Key Benefits

1. **🧹 Clean Organization**: No more cluttered root directory
2. **🔍 Easy Navigation**: Find files by type and purpose
3. **📊 Better Analytics**: Structured delay and performance reports
4. **🔄 Scalable**: Handles single PO to thousands efficiently
5. **🛠️ Maintainable**: Modular codebase with clear separation of concerns
6. **📋 Auditable**: Full transparency in calculation methodology

---

**Transforms complex Excel formulas into organized business intelligence for operational excellence.**
