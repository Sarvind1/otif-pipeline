"""
TAT Calculator Main Module
=========================

Main TATCalculator class that coordinates all sub-modules.
Simplified to use new method-based calculations without separate delay calculator.
"""

import logging
from typing import List, Dict, Any
import pandas as pd
from models_config import load_config, validate_config
from expression_evaluator import ExpressionEvaluator
from stage_calculator import StageCalculator
from tat_processor import TATProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TATCalculator:
    """
    Core TAT calculation engine that processes PO data through configurable stages.
    
    This is the main entry point that coordinates all sub-modules:
    - Configuration management (models_config)
    - Expression evaluation (expression_evaluator)
    - Stage calculations (stage_calculator)
    - TAT processing and export (tat_processor)
    
    Uses simplified method-based approach (Projected/Actual/Adjusted).
    """
    
    def __init__(self, config_path: str = "stages_config.json"):
        """
        Initialize the TAT Calculator
        
        Args:
            config_path: Path to the stages configuration JSON file
        """
        # Load and validate configuration
        self.config = load_config(config_path)
        validate_config(self.config)
        
        # Initialize sub-components
        self.expression_evaluator = ExpressionEvaluator()
        self.stage_calculator = StageCalculator(self.config, self.expression_evaluator)
        self.tat_processor = TATProcessor(self.config, self.stage_calculator)
        
        logger.info(f"TAT Calculator initialized with {len(self.config.stages)} stages")
    
    def calculate_tat(self, po_row: pd.Series) -> Dict[str, Any]:
        """
        Calculate TAT for all stages of a PO with integrated delay information
        
        Args:
            po_row: Pandas Series containing PO data
            
        Returns:
            Dictionary with complete TAT calculation results including delay info
        """
        return self.tat_processor.calculate_tat(po_row)
    
    def process_batch(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Process multiple POs in batch
        
        Args:
            df: DataFrame containing multiple PO rows
            
        Returns:
            List of TAT calculation results
        """
        return self.tat_processor.process_batch(df)
    
    def export_stage_level_excel(self, df: pd.DataFrame, results: List[Dict[str, Any]], output_file: str):
        """
        Export stage-level data to Excel with 5 separate tabs:
        - Method: Method used for each stage (Projected/Actual/Adjusted)
        - Actual_Timestamps: Actual timestamps for each stage
        - Target_Timestamps: Target timestamps for each stage
        - Final_Timestamps: Final timestamps used for each stage
        - Delay: Delay in days for each stage
        
        Args:
            df: Original DataFrame
            results: TAT calculation results
            output_file: Output Excel file path
        """
        self.tat_processor.export_stage_level_excel(df, results, output_file)
    
    # Convenience methods for backward compatibility
    def _get_date_value(self, field_name: str, po_row: pd.Series):
        """Legacy method for backward compatibility"""
        return self.expression_evaluator.get_date_value(field_name, po_row)
    
    def _evaluate_expression(self, expression: str, po_row: pd.Series):
        """Legacy method for backward compatibility"""
        return self.expression_evaluator.evaluate_expression(expression, po_row)
    
    def calculate_adjusted_timestamp(self, stage_id: str, po_row: pd.Series):
        """Legacy method for backward compatibility"""
        return self.stage_calculator.calculate_adjusted_timestamp(stage_id, po_row)


if __name__ == "__main__":
    print("TAT Calculator System - Simplified Method-Based Approach")
    print("=" * 56)
    print("Key Features:")
    print("✅ Method-based calculations (Projected/Actual/Adjusted)")
    print("✅ Integrated delay calculation")
    print("✅ 5-tab Excel export")
    print("✅ Simplified JSON output")
    print()
    print("Usage Examples:")
    print("1. Basic TAT calculation:")
    print("   calculator = TATCalculator()")
    print("   result = calculator.calculate_tat(po_row)")
    print()
    print("2. Batch processing:")
    print("   results = calculator.process_batch(df)")
    print()
    print("3. Excel export (5 tabs):")
    print("   calculator.export_stage_level_excel(df, results, 'outputs/excel_exports/stage_level.xlsx')")
    print("   # Creates tabs: Method, Actual_Timestamps, Target_Timestamps, Final_Timestamps, Delay")
