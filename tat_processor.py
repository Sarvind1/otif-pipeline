"""
TAT Processor Module
====================

Main TAT processing orchestrator with batch processing and export capabilities.
Simplified to support new method-based calculation structure.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path
import pandas as pd
from models_config import StagesConfig
from stage_calculator import StageCalculator

logger = logging.getLogger(__name__)


class TATProcessor:
    """
    Orchestrates TAT calculations across all stages and handles output formatting.
    Supports new simplified method-based calculations.
    """
    
    def __init__(self, config: StagesConfig, stage_calculator: StageCalculator):
        self.config = config
        self.stage_calculator = stage_calculator
        # Ensure organized output folders exist
        self._ensure_output_folders()
    
    def _ensure_output_folders(self):
        """Ensure all output folders exist"""
        folders = [
            'outputs/tat_results',
            'outputs/excel_exports',
            'outputs/csv_files',
            'outputs/logs'
        ]
        
        for folder in folders:
            Path(folder).mkdir(parents=True, exist_ok=True)
    
    def calculate_tat(self, po_row: pd.Series) -> Dict[str, Any]:
        """
        Calculate TAT for all stages of a PO using new simplified logic
        
        Args:
            po_row: Pandas Series containing PO data
            
        Returns:
            Dictionary with complete TAT calculation results
        """
        # Clear cache for new calculation
        self.stage_calculator.reset_cache()
        
        result = {
            "po_id": po_row.get('po_razin_id', 'Unknown'),
            "calculation_date": datetime.now().isoformat(),
            "summary": {
                "total_stages": len(self.config.stages),
                "calculated_stages": 0,
                "methods_used": {
                    "Projected": 0,
                    "Actual": 0,
                    "Adjusted": 0,
                    "error": 0
                },
                "stages_with_delays": 0,
                "total_delay_days": 0
            },
            "stages": {}
        }
        
        # Calculate each stage
        for stage_id, stage_config in self.config.stages.items():
            calc_details = self.stage_calculator.calculate_adjusted_timestamp(stage_id, po_row)
            
            # Update summary statistics
            if calc_details["final_timestamp"] is not None:
                result["summary"]["calculated_stages"] += 1
            
            method = calc_details.get("method", "error")
            if method in result["summary"]["methods_used"]:
                result["summary"]["methods_used"][method] += 1
            
            # Update delay statistics
            if calc_details.get("delay") is not None:
                result["summary"]["stages_with_delays"] += 1
                result["summary"]["total_delay_days"] += calc_details["delay"]
            
            # Create stage result
            stage_result = {
                "name": stage_config.name,
                "method": method,
                "target_timestamp": calc_details.get("target_timestamp"),
                "actual_timestamp": calc_details.get("actual_timestamp"),
                "final_timestamp": calc_details.get("final_timestamp"),
                "delay": calc_details.get("delay"),
                "lead_time": calc_details.get("lead_time_applied"),
                "status": calc_details.get("status"),
                "calculation_source": calc_details.get("calculation_source"),
                "process_flow": {
                    "team_owner": stage_config.process_flow.team_owner,
                    "process_type": stage_config.process_flow.process_type,
                    "critical_path": stage_config.process_flow.critical_path,
                    "handoff_points": stage_config.process_flow.handoff_points
                },
                "dependencies": calc_details.get("dependencies", [])
            }
            
            result["stages"][stage_id] = stage_result
        
        # Calculate completion rate
        result["summary"]["completion_rate"] = round(
            result["summary"]["calculated_stages"] / result["summary"]["total_stages"] * 100, 2
        ) if result["summary"]["total_stages"] > 0 else 0
        
        # Calculate average delay
        if result["summary"]["stages_with_delays"] > 0:
            result["summary"]["average_delay_days"] = round(
                result["summary"]["total_delay_days"] / result["summary"]["stages_with_delays"], 2
            )
        
        return result
    
    def process_batch(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Process multiple POs in batch
        
        Args:
            df: DataFrame containing multiple PO rows
            
        Returns:
            List of TAT calculation results
        """
        results = []
        
        for index, row in df.iterrows():
            try:
                result = self.calculate_tat(row)
                results.append(result)
                logger.info(f"Processed PO: {result['po_id']}")
            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")
                results.append({
                    "po_id": row.get('po_razin_id', f'Row_{index}'),
                    "error": str(e),
                    "calculation_date": datetime.now().isoformat()
                })
        
        return results

    def export_stage_level_excel(self, df: pd.DataFrame, results: List[Dict[str, Any]], output_file: str):
        """
        Export stage-level data to Excel with 7 separate tabs:
        - Method: Method used for each stage (Projected/Actual/Adjusted)
        - Actual_Timestamps: Actual timestamps for each stage
        - Target_Timestamps: Target timestamps for each stage
        - Final_Timestamps: Final timestamps used for each stage
        - Delay: Delay in days for each stage
        - status: Whether preceding stages were Projected or Actual/Adjusted
        - Calculation_Source: How the final timestamp was calculated
        
        Args:
            df: Original DataFrame
            results: TAT calculation results
            output_file: Output Excel file path
        """
        # Ensure the directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get all stage configurations
        stage_configs = {stage_id: config for stage_id, config in self.config.stages.items()}
        
        # Prepare data for each tab
        po_ids = []
        method_data = {}
        actual_timestamps_data = {}
        target_timestamps_data = {}
        final_timestamps_data = {}
        delay_data = {}
        status_data = {}
        calculation_source_data = {}
        
        # Initialize stage columns
        for stage_id, stage_config in stage_configs.items():
            stage_name = stage_config.name
            method_data[stage_name] = []
            actual_timestamps_data[stage_name] = []
            target_timestamps_data[stage_name] = []
            final_timestamps_data[stage_name] = []
            delay_data[stage_name] = []
            status_data[stage_name] = []
            calculation_source_data[stage_name] = []
        
        # Process each result
        for result in results:
            if 'stages' not in result:
                continue
                
            po_id = result['po_id']
            po_ids.append(po_id)
            
            # Process each stage
            for stage_id, stage_config in stage_configs.items():
                stage_name = stage_config.name
                stage_result = result['stages'].get(stage_id, {})
                
                # 1. Method
                method = stage_result.get('method', '')
                method_data[stage_name].append(method)
                
                # 2. Actual timestamps
                actual_timestamp = None
                if stage_result.get('actual_timestamp'):
                    actual_timestamp = pd.to_datetime(stage_result['actual_timestamp']).date()
                actual_timestamps_data[stage_name].append(actual_timestamp)
                
                # 3. Target timestamps
                target_timestamp = None
                if stage_result.get('target_timestamp'):
                    target_timestamp = pd.to_datetime(stage_result['target_timestamp']).date()
                target_timestamps_data[stage_name].append(target_timestamp)
                
                # 4. Final timestamps
                final_timestamp = None
                if stage_result.get('final_timestamp'):
                    final_timestamp = pd.to_datetime(stage_result['final_timestamp']).date()
                final_timestamps_data[stage_name].append(final_timestamp)
                
                # 5. Delay days
                delay_days = stage_result.get('delay')
                delay_data[stage_name].append(delay_days)
                
                
                status = stage_result.get('status', '')
                status_data[stage_name].append(status)
                
                # 7. Calculation source
                calculation_source = stage_result.get('calculation_source', '')
                calculation_source_data[stage_name].append(calculation_source)
        
        # Create DataFrames for each tab
        method_df = pd.DataFrame({'PO_ID': po_ids, **method_data})
        actual_df = pd.DataFrame({'PO_ID': po_ids, **actual_timestamps_data})
        target_df = pd.DataFrame({'PO_ID': po_ids, **target_timestamps_data})
        final_df = pd.DataFrame({'PO_ID': po_ids, **final_timestamps_data})
        delay_df = pd.DataFrame({'PO_ID': po_ids, **delay_data})
        precedence_df = pd.DataFrame({'PO_ID': po_ids, **status_data})
        calc_source_df = pd.DataFrame({'PO_ID': po_ids, **calculation_source_data})
        
        # Write to Excel with multiple tabs
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            method_df.to_excel(writer, sheet_name='Method', index=False)
            actual_df.to_excel(writer, sheet_name='Actual_Timestamps', index=False)
            target_df.to_excel(writer, sheet_name='Target_Timestamps', index=False)
            final_df.to_excel(writer, sheet_name='Final_Timestamps', index=False)
            delay_df.to_excel(writer, sheet_name='Delay', index=False)
            precedence_df.to_excel(writer, sheet_name='status', index=False)
            calc_source_df.to_excel(writer, sheet_name='Calculation_Source', index=False)
        
        logger.info(f"Stage-level results exported to: {output_file}")
        logger.info(f"  - Method tab: {len(po_ids)} POs x {len(stage_configs)} stages")
        logger.info(f"  - Actual_Timestamps tab: {len(po_ids)} POs x {len(stage_configs)} stages")
        logger.info(f"  - Target_Timestamps tab: {len(po_ids)} POs x {len(stage_configs)} stages")
        logger.info(f"  - Final_Timestamps tab: {len(po_ids)} POs x {len(stage_configs)} stages")
        logger.info(f"  - Delay tab: {len(po_ids)} POs x {len(stage_configs)} stages")
        logger.info(f"  - status tab: {len(po_ids)} POs x {len(stage_configs)} stages")
        logger.info(f"  - Calculation_Source tab: {len(po_ids)} POs x {len(stage_configs)} stages")
    
    def save_to_csv(self, df: pd.DataFrame, filename_prefix: str = "processed_data") -> str:
        """
        Save processed DataFrame to organized CSV folder
        
        Args:
            df: DataFrame to save
            filename_prefix: Prefix for filename
            
        Returns:
            Full path of saved file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/csv_files/{filename_prefix}_{timestamp}.csv"
        
        # Ensure directory exists
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(filename, index=False)
        logger.info(f"CSV saved to: {filename}")
        return filename
