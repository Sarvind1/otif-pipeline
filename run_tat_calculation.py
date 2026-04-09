"""
Complete TAT Calculation Runner
==============================

This script runs the complete TAT calculation system on your Excel data
and generates comprehensive reports and analytics.
"""

import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
import traceback
import os

# sys.stdout = open(os.devnull, 'w')

# Set up organized folder structure
def setup_output_folders():
    """Create organized output folder structure"""
    folders = [
        'outputs/tat_results',
        'outputs/excel_exports',
        'outputs/csv_files',
        'outputs/logs'
    ]
    
    for folder in folders:
        Path(folder).mkdir(parents=True, exist_ok=True)
    
    return folders

# Create folders before setting up logging
setup_output_folders()

# Set up comprehensive logging with organized paths
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outputs/logs/tat_calculation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TATRunner:
    """Complete TAT calculation runner with enhanced reporting and organized outputs"""
    
    def __init__(self, excel_file: str = "ts_big.xlsx", config_file: str = "stages_config.json"):
        self.excel_file = excel_file
        self.config_file = config_file
        self.df = None
        self.calculator = None
        self.results = []
        
        # Ensure output folders exist
        self.output_folders = setup_output_folders()
        logger.info(f"Output folders created: {self.output_folders}")
        
    def setup(self):
        """Set up the calculator and load data"""
        logger.info("Setting up TAT calculation environment...")
        
        # Load Excel data
        self.load_excel_data()
        
        # Initialize calculator
        self.initialize_calculator()
        
        logger.info("Setup completed successfully!")
        
    def load_excel_data(self):
        """Load and prepare Excel data"""
        logger.info(f"Loading Excel file: {self.excel_file}")
        
        try:
            self.df = pd.read_excel(self.excel_file)
            logger.info(f"Loaded {len(self.df)} rows and {len(self.df.columns)} columns")
            
            # Clean column names
            self.df.columns = self.df.columns.str.strip()
            
            # Convert date columns
            self.convert_date_columns()
            
            # Validate required columns
            self.validate_required_columns()
            
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            raise
            
    def convert_date_columns(self):
        """Convert relevant columns to datetime"""
        date_columns = [
            'po_created_date', 'po_approval_date', 'supplier_confirmation_date',
            'pi_invoice_approval_date', 'pi_payment_date', 'receive_first_prd_date',
            'prd_reconfirmed_date', 'po_im_date_value', 'po_sm_date_value',
            'batch_created_ts', 'sm_signoff_ts', 'ci_invoice_approval_date',
            'ci_payment_date', 'qc_schedule_date', 'ffw_booking_ts', 'spd_ts',
            'stock_pickup_date', 'shipment_creation_date', 'shipment_in_transit_date',
            'bi_invoice_approval_date', 'bi_payment_date', 'ffwp_telex_release_date',
            'shipment_stock_delivery_date', 'item_receipt_date', 'actual_cargo_pick_up_date',
            'actual_shipping_date', 'actual_arrival_date', 'actual_delivery_date'
        ]
        
        for col in date_columns:
            if col in self.df.columns:
                original_type = self.df[col].dtype
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                self.df[col] = self.df[col].astype(object).where(pd.notna(self.df[col]), None)

                missing_count = self.df[col].isna().sum()
                total_count = len(self.df)
                
                logger.info(f"Converted {col}: {original_type} -> datetime, "
                          f"{missing_count}/{total_count} missing ({missing_count/total_count*100:.1f}%)")
    
    def validate_required_columns(self):
        """Validate that required columns exist"""
        required_columns = ['po_razin_id', 'po_created_date']
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        logger.info("All required columns present")
    
    def initialize_calculator(self):
        """Initialize the TAT calculator"""
        logger.info(f"Initializing TAT Calculator with config: {self.config_file}")
        
        try:
            from tat_calculator_main import TATCalculator
            self.calculator = TATCalculator(self.config_file)
            logger.info("TAT Calculator initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing calculator: {e}")
            raise
    
    def run_calculations(self, sample_size: int = None):
        """Run TAT calculations"""
        if sample_size:
            df_to_process = self.df.head(sample_size)
            logger.info(f"Processing sample of {sample_size} POs")
        else:
            df_to_process = self.df
            logger.info(f"Processing all {len(df_to_process)} POs")
        
        self.results = []
        errors = []
        
        # Process batch
        try:
            self.results = self.calculator.process_batch(df_to_process)
            logger.info(f"Completed calculations: {len(self.results)} results")
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            # Fallback to individual processing
            for index, row in df_to_process.iterrows():
                try:
                    po_id = row.get('po_razin_id', f'Row_{index}')
                    logger.info(f"Processing PO {index + 1}/{len(df_to_process)}: {po_id}")
                    
                    result = self.calculator.calculate_tat(row)
                    self.results.append(result)
                    
                except Exception as e:
                    error_info = {
                        'index': index,
                        'po_id': row.get('po_razin_id', f'Row_{index}'),
                        'error': str(e),
                        'traceback': traceback.format_exc()
                    }
                    errors.append(error_info)
                    logger.error(f"Error processing row {index}: {e}")
        
        if errors:
            self.save_errors(errors)
        
        return self.results
    
    def save_errors(self, errors):
        """Save error details to organized logs folder"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_file = f"outputs/logs/tat_errors_{timestamp}.json"
        
        with open(error_file, 'w') as f:
            json.dump(errors, f, indent=2)
        logger.info(f"Error details saved to: {error_file}")
    
    def save_results(self, filename_prefix: str = "tat_results"):
        """Save TAT calculation results"""
        if not self.results:
            logger.warning("No results to save")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/tat_results/{filename_prefix}_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        logger.info(f"TAT results saved to: {filename}")
        return filename
    
    def export_stage_level_excel(self, filename_prefix: str = "stage_level_analysis"):
        """Export stage-level data to Excel with 7 tabs"""
        if not self.results:
            logger.warning("No results to export")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/excel_exports/{filename_prefix}_{timestamp}.xlsx"
        
        self.calculator.export_stage_level_excel(self.df, self.results, filename)
        return filename
    
    def save_processed_csv(self, filename_prefix: str = "processed_data"):
        """Save processed CSV data to organized csv_files folder"""
        if self.df is None:
            logger.warning("No data to save as CSV")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/csv_files/{filename_prefix}_{timestamp}.csv"
        
        self.df.to_csv(filename, index=False)
        logger.info(f"Processed CSV saved to: {filename}")
        return filename
    
    def print_summary(self):
        """Print a summary of results"""
        if not self.results:
            print("No results available for summary")
            return
        
        print("\n📊 TAT Calculation Summary:")
        print("=" * 50)
        
        total_stages_with_delays = 0
        total_delay_days = 0
        methods_count = {"Projected": 0, "Actual": 0, "Adjusted": 0}
        
        for result in self.results:
            if 'stages' not in result:
                continue
                
            po_id = result['po_id']
            summary = result.get('summary', {})
            
            # Count methods
            for method, count in summary.get('methods_used', {}).items():
                if method in methods_count:
                    methods_count[method] += count
            
            # Count delays
            stages_with_delays = summary.get('stages_with_delays', 0)
            delay_days = summary.get('total_delay_days', 0)
            
            total_stages_with_delays += stages_with_delays
            total_delay_days += delay_days
            
            if stages_with_delays > 0:
                print(f"PO {po_id}: {stages_with_delays} delayed stages, "
                      f"{delay_days} total delay days")
        
        print(f"\n🎯 Overall Statistics:")
        print(f"   Total POs: {len(self.results)}")
        print(f"   Stages with delays: {total_stages_with_delays}")
        print(f"   Total delay days: {total_delay_days}")
        print(f"\n📊 Methods Used:")
        for method, count in methods_count.items():
            print(f"   {method}: {count}")


def main(filen="ts_big.xlsx"):
    """Main execution function"""
    print("TAT Calculation System - Starting...")
    print("=" * 70)
    
    try:
        # Initialize runner with ts_small_1.xlsx
        runner = TATRunner(excel_file=filen)
        
        # Setup
        runner.setup()
        
        # Run calculations (process all)
        print("\nRunning calculations...")
        results = runner.run_calculations()
        print(results)
        
        if results:
            # Print summary
            runner.print_summary()
            
            # Save results to organized folders
            results_file = runner.save_results()
            processed_csv_file = runner.save_processed_csv()
            
            # Export to Excel
            stage_level_file = runner.export_stage_level_excel()
            
        #     print(f"\n📁 Output Files:")
        #     print(f"├── TAT Results: {results_file}")
        #     print(f"├── Excel Export: {stage_level_file}")
        #     print(f"│   └── 7 tabs:")
        #     print(f"│       ├── Method: Shows Projected/Actual/Adjusted")
        #     print(f"│       ├── Actual_Timestamps: Actual dates from data")
        #     print(f"│       ├── Target_Timestamps: Calculated target dates")
        #     print(f"│       ├── Final_Timestamps: Final dates used")
        #     print(f"│       ├── Delay: Delay in days (negative = early)")
        #     print(f"│       ├── Precedence_Method: Projected or Actual/Adjusted")
        #     print(f"│       └── Calculation_Source: How final was calculated")
        #     print(f"└── CSV File: {processed_csv_file}")
            
        #     print(f"\n💡 Debugging Features:")
        #     print(f"   - Calculation_Source tab shows exactly how each timestamp was determined")
        #     print(f"   - Precedence_Method helps track if delays propagate from preceding stages")
        #     print(f"   - Method tab clearly shows which stages have actual data vs projections")
            
        # print("\n✅ TAT Calculation completed successfully!")

        return stage_level_file
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"Error: {e}")
        print("See outputs/logs/tat_calculation.log for detailed error information")

    return stage_level_file


# if __name__ == "__main__":
#     main()
