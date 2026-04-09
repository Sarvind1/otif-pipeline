"""
Stage-Level Excel Export Example
================================

Example script demonstrating how to use the new stage-level Excel export
functionality with 3 tabs: actual_timestamps, timestamps, delay_days
"""

from tat_calculator_main import TATCalculator
import pandas as pd
from datetime import datetime

def example_stage_level_export():
    """Example of how to generate stage-level Excel export"""
    
    print("Stage-Level Excel Export Example")
    print("=" * 40)
    
    # Initialize calculator
    calculator = TATCalculator("stages_config.json")
    
    # Load sample data (replace with your Excel file)
    try:
        df = pd.read_excel("ts_small.xlsx")
        print(f"✅ Loaded {len(df)} POs for processing")
    except FileNotFoundError:
        print("❌ Excel file not found. Please ensure ts_small.xlsx or ts_big.xlsx is available.")
        return
    
    # Process POs (using small sample for demo)
    sample_size = 3
    results = calculator.process_batch(df.head(sample_size))
    
    print(f"✅ Processed {len(results)} POs successfully")
    
    # Generate stage-level Excel export
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"outputs/excel_exports/stage_level_example_{timestamp}.xlsx"
    
    calculator.export_stage_level_excel(df.head(sample_size), results, output_file)
    
    print(f"\n📊 Stage-Level Excel Generated: {output_file}")
    print("\n📋 Excel Structure:")
    print("├── Tab 1: actual_timestamps")
    print("│   └── Actual timestamps from PO data")
    print("├── Tab 2: timestamps") 
    print("│   └── Calculated timestamps from TAT processing")
    print("└── Tab 3: delay_days")
    print("    └── Delay days for each stage")
    
    print(f"\n🎯 Each tab contains:")
    print(f"   - Rows: PO IDs ({sample_size} POs)")
    print(f"   - Columns: All 31 stages")
    print(f"   - Format: Matrix layout for easy analysis")
    
    # Show example data structure
    if results:
        example_po = results[0]['po_id']
        stage_count = len(results[0].get('stages', {}))
        print(f"\n💡 Example Matrix Layout:")
        print(f"   PO_ID       │ 01. PO Approval │ 02. Supplier... │ ... ({stage_count} stages)")
        print(f"   ────────────┼─────────────────┼─────────────────┼─────────────────")
        print(f"   {example_po[:12]:<12}│ 2025-06-01      │ 2025-06-03      │ ...")
        
    print(f"\n✅ Stage-level analysis complete!")
    return output_file


def explain_tabs():
    """Explain what each tab contains"""
    
    print("\n📚 Tab Explanations:")
    print("=" * 40)
    
    print("\n📋 Tab 1: actual_timestamps")
    print("   Purpose: Shows actual timestamps from your PO data")
    print("   Source: Original Excel columns (e.g., po_created_date, pi_payment_date)")
    print("   Use Case: Compare what actually happened vs what was calculated")
    
    print("\n⏱️  Tab 2: timestamps")
    print("   Purpose: Shows calculated timestamps from TAT processing")
    print("   Source: TAT calculation engine results")
    print("   Use Case: See what the system calculated as expected dates")
    
    print("\n📊 Tab 3: delay_days")
    print("   Purpose: Shows delay days for each stage")
    print("   Calculation: Actual timestamp - Calculated timestamp")
    print("   Values:")
    print("     • Positive number = Delayed (e.g., 5 = 5 days late)")
    print("     • Negative number = Early (e.g., -2 = 2 days early)")
    print("     • 0 = On time")
    print("     • Blank/Null = No data available")
    
    print("\n🎯 Analysis Tips:")
    print("   • Compare tabs to identify patterns")
    print("   • Look for stages with consistent delays")
    print("   • Identify bottleneck stages")
    print("   • Track performance across POs")


if __name__ == "__main__":
    # Run the example
    output_file = example_stage_level_export()
    
    # Explain the tabs
    explain_tabs()
    
    print(f"\n🚀 Next Steps:")
    print(f"   1. Open Excel file: {output_file}")
    print(f"   2. Review each of the 3 tabs")
    print(f"   3. Use data for stage-level analysis")
    print(f"   4. To process all POs, use: python run_tat_calculation.py")
