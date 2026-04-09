"""
TAT (Turnaround Time) Calculator System
=====================================

A comprehensive system for calculating adjusted timestamps for Purchase Order (PO) 
processing workflow stages using a configurable, dependency-driven approach.

This implementation replaces complex Excel formulas with maintainable Python code
that supports dynamic expressions, stage dependencies, and transparent reasoning.
"""

import json
import ast
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
from pathlib import Path
import pandas as pd
from pydantic import BaseModel, Field, validator
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProcessFlow(BaseModel):
    """Process flow metadata for a stage"""
    critical_path: bool
    parallel_processes: List[str] = Field(default_factory=list)
    handoff_points: List[str] = Field(default_factory=list)
    process_type: str
    team_owner: str


class FallbackCalculation(BaseModel):
    """Fallback calculation configuration"""
    expression: str
    



class StageConfig(BaseModel):
    """Configuration for a single stage"""
    name: str
    actual_timestamp: Optional[str] = None
    preceding_stage: Optional[Union[str, List[str]]] = None
    process_flow: ProcessFlow
    fallback_calculation: FallbackCalculation
    lead_time: int = Field(ge=0, description="Lead time in days")
    



class StagesConfig(BaseModel):
    """Complete stages configuration"""
    stages: Dict[str, StageConfig]
    
    @validator('stages')
    def validate_stage_ids(cls, v):
        """Ensure all stage IDs are valid"""
        for stage_id in v.keys():
            if not stage_id.strip():
                raise ValueError("Stage ID cannot be empty")
        return v


class TATCalculator:
    """
    Core TAT calculation engine that processes PO data through configurable stages.
    
    Features:
    - Configuration-driven stage definitions
    - Dynamic expression evaluation with custom functions
    - Dependency resolution with cycle detection
    - Memoization for performance optimization
    - Comprehensive audit trails and reasoning
    """
    
    def __init__(self, config_path: str = "stages_config.json"):
        """
        Initialize the TAT Calculator
        
        Args:
            config_path: Path to the stages configuration JSON file
        """
        self.config = self._load_config(config_path)
        self._validate_config()
        self.calculated_adjustments: Dict[str, Tuple[Optional[datetime], Dict[str, Any]]] = {}
        
    def _load_config(self, config_path: str) -> StagesConfig:
        """Load and validate configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
            return StagesConfig(**config_data)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def _validate_config(self):
        """Validate configuration for circular dependencies"""
        # Build dependency graph
        graph = {}
        for stage_id, stage in self.config.stages.items():
            graph[stage_id] = stage.preceding_stage or []
        
        # Check for cycles using DFS
        visited = set()
        rec_stack = set()
        
        def has_cycle(node):
            if node in rec_stack:
                return True
            if node in visited:
                return False
                
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, []):
                if neighbor in graph and has_cycle(neighbor):
                    return True
            
            rec_stack.remove(node)
            return False
        
        for stage_id in graph:
            if stage_id not in visited:
                if has_cycle(stage_id):
                    raise ValueError(f"Circular dependency detected involving stage {stage_id}")
    
    def _get_date_value(self, field_name: str, po_row: pd.Series) -> Optional[datetime]:
        """
        Extract datetime value from PO data with robust handling
        
        Args:
            field_name: Name of the field to extract
            po_row: Pandas Series containing PO data
            
        Returns:
            datetime object or None if not available/valid
        """
        if field_name not in po_row.index:
            logger.warning(f"Field '{field_name}' not found in PO data")
            return None
            
        value = po_row[field_name]
        
        # Handle various data types
        if pd.isna(value) or value == "" or value == "NA":
            return None
            
        if isinstance(value, datetime):
            return value
            
        if isinstance(value, str):
            try:
                # Try common datetime formats
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%d/%m/%Y"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                        
                # Try pandas datetime parsing as fallback
                return pd.to_datetime(value)
            except:
                logger.warning(f"Could not parse date from field '{field_name}': {value}")
                return None
                
        return None
    
    def _evaluate_expression(self, expression: str, po_row: pd.Series) -> Tuple[Optional[datetime], str]:
        """
        Evaluate dynamic expressions with custom functions
        
        Supported functions:
        - max(date1, date2, ...): Returns latest datetime
        - add_days(date, days): Adds days to a date
        
        Args:
            expression: Expression string to evaluate
            po_row: PO data for field resolution
            
        Returns:
            Tuple of (result_datetime, formula_description)
        """
        try:
            # Parse expression into AST
            tree = ast.parse(expression, mode='eval')
            result = self._eval_node(tree.body, po_row)
            
            if isinstance(result, datetime):
                return result, f"Calculation: {expression} = {result.strftime('%Y-%m-%d %H:%M:%S')}"
            else:
                return None, f"Calculation failed: {expression}"
                
        except Exception as e:
            logger.error(f"Error evaluating expression '{expression}': {e}")
            return None, f"Calculation error: {expression} ({str(e)})"
    

    def _eval_node(self, node: ast.AST, po_row: pd.Series) -> Any:
        """Recursively evaluate AST nodes"""
        if isinstance(node, ast.Name):
            var_name = node.id
            # Check if it's a stage reference
            if var_name.startswith('stage_'):
                print ("Veriable name is", var_name)
                stage_id = var_name.replace('stage_', '')
                if stage_id in self.calculated_adjustments:
                    timestamp, _ = self.calculated_adjustments[stage_id]
                    return timestamp  # Already a datetime or None
            
            return po_row.get(node.id)
        
        elif isinstance(node, ast.Constant):
            return node.value
        
        elif isinstance(node, ast.List):
            # Handle list literals like ['5'], ['2']
            return [self._eval_node(elt, po_row) for elt in node.elts]
        
        elif isinstance(node, ast.BinOp):
            # Handle binary operations like +, -, *, /
            left = self._eval_node(node.left, po_row)
            right = self._eval_node(node.right, po_row)
            
            if left is None or right is None:
                return None
            
            # Handle datetime arithmetic
            if isinstance(node.op, ast.Add):
                # For datetime + number (days)
                if isinstance(left, datetime) and isinstance(right, (int, float)):
                    return left + timedelta(days=right)
                elif isinstance(right, datetime) and isinstance(left, (int, float)):
                    return right + timedelta(days=left)
                else:
                    return left + right
                    
            elif isinstance(node.op, ast.Sub):
                # For datetime - number (days)
                if isinstance(left, datetime) and isinstance(right, (int, float)):
                    return left - timedelta(days=right)
                # For datetime - datetime (returns days)
                elif isinstance(left, datetime) and isinstance(right, datetime):
                    return (left - right).days
                else:
                    return left - right
                    
            elif isinstance(node.op, ast.Mult):
                return left * right
                
            elif isinstance(node.op, ast.Div):
                return left / right if right != 0 else None
                
            else:
                logger.warning(f"Unsupported binary operation: {type(node.op)}")
                return None
    
        
        
        elif isinstance(node, ast.Compare):
            # Handle comparisons like pi_applicable==1
            left = self._eval_node(node.left, po_row)
            op = node.ops[0]
            right = self._eval_node(node.comparators[0], po_row)
            
            if isinstance(op, ast.Eq):
                return left == right
            elif isinstance(op, ast.NotEq):
                return left != right
            elif isinstance(op, ast.Lt):
                return left < right
            elif isinstance(op, ast.LtE):
                return left <= right
            elif isinstance(op, ast.Gt):
                return left > right
            elif isinstance(op, ast.GtE):
                return left >= right
            else:
                raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")
        
        
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
                
                # Handle iff with lazy evaluation
                if func_name == 'iff' or func_name == 'cond':
                    if len(node.args) == 3:
                        condition = self._eval_node(node.args[0], po_row)
                        if condition:
                            return self._eval_node(node.args[1], po_row)
                        else:
                            return self._eval_node(node.args[2], po_row)
                    raise ValueError("iff requires exactly 3 arguments")
                
                # For other functions, evaluate arguments first
                args = [self._eval_node(arg, po_row) for arg in node.args]
                
                if func_name == 'max':
                    valid_dates = [arg for arg in args if isinstance(arg, datetime)]
                    return max(valid_dates) if valid_dates else None
                
                elif func_name == 'add_days':
                    if len(args) >= 2 and isinstance(args[0], datetime) and isinstance(args[1], (int, float)):
                        return args[0] + timedelta(days=int(args[1]))
                    return None
                
                else:
                    raise ValueError(f"Unknown function: {func_name}")
        
        else:
            raise ValueError(f"Unsupported AST node type: {type(node).__name__}")

    def calculate_adjusted_timestamp(self, stage_id: str, po_row: pd.Series) -> Tuple[Optional[datetime], Dict[str, Any]]:
        """
        Calculate adjusted timestamp for a specific stage using priority logic:
        
        1. Precedence-based calculation (from dependencies + lead time)
        2. Actual timestamp comparison (if available)
        3. Fallback calculation (last resort)
        
        Args:
            stage_id: ID of the stage to calculate
            po_row: PO data row
            
        Returns:
            Tuple of (calculated_timestamp, calculation_details)
        """
        # Check if already calculated (memoization)
        if stage_id in self.calculated_adjustments:
            return self.calculated_adjustments[stage_id]
        
        if stage_id not in self.config.stages:
            logger.error(f"Stage {stage_id} not found in configuration")
            return None, {"method": "error", "reason": f"Stage {stage_id} not found"}
        
        stage = self.config.stages[stage_id]
        
        # Initialize calculation details
        calc_details = {
            "method": None,
            "source": None,
            "target_date": None,
            "lead_time_applied": stage.lead_time,
            "decision_reason": None,
            "dependencies": [],
            "actual_field": None,
            "actual_value": None,
            "precedence_value": None,
            "final_choice": None
        }
        
        # In calculate_adjusted_timestamp, replace the complex preceding_stage handling with:

        # 1. Calculate precedence-based timestamp
        precedence_timestamp = None
        if stage.preceding_stage:
            dependencies = []
            preceding_timestamps = []
            
            # Just evaluate the string expression - it always returns a list
            result = self._eval_node(ast.parse(stage.preceding_stage, mode='eval').body, po_row)
            preceding_stage_ids = result if isinstance(result, list) else []
            
            # Process the stage IDs
            for prec_stage_id in preceding_stage_ids:
                prec_stage_id = str(prec_stage_id)
                if prec_stage_id in self.config.stages:
                    prec_timestamp, prec_details = self.calculate_adjusted_timestamp(prec_stage_id, po_row)
                    if prec_timestamp:
                        preceding_timestamps.append(prec_timestamp)
                        dependencies.append({
                            "stage_id": prec_stage_id,
                            "stage_name": self.config.stages[prec_stage_id].name,
                            "timestamp": prec_timestamp.isoformat(),
                            "method": prec_details["method"] if isinstance(prec_details, dict) else "legacy"
                        })
            
            calc_details["dependencies"] = dependencies
            if preceding_timestamps:
                base_timestamp = max(preceding_timestamps)
                precedence_timestamp = base_timestamp #
                calc_details["precedence_value"] = precedence_timestamp.isoformat()
                calc_details["target_date"] = (base_timestamp + timedelta(days=stage.lead_time)).isoformat()
        
        # 2. Extract and get actual timestamp (evaluate as expression or field)
        if not precedence_timestamp:
            fallback_result, fallback_formula = self._evaluate_expression(
                stage.fallback_calculation.expression, po_row
            )
            print ("Fallback", fallback_result)
            if fallback_result:
                final_timestamp = fallback_result + timedelta(days=stage.lead_time)
                calc_details["source"] = stage.fallback_calculation.expression
                calc_details["target_date"] = fallback_result.isoformat()
                calc_details["decision_reason"] = "No precedence available, using fallback expression"
                calc_details["final_choice"] = "fallback"
            else:
                calc_details["method"] = "failed"
                calc_details["decision_reason"] = "No valid calculation method available"
        
        
        actual_timestamp = None
        actual_formula = None
        if stage.actual_timestamp:
            actual_timestamp, actual_formula = self._evaluate_expression(stage.actual_timestamp, po_row)
            calc_details["actual_field"] = stage.actual_timestamp
            if actual_timestamp:
                calc_details["actual_value"] = actual_timestamp.isoformat()
        
        # 3. Determine final timestamp and method
        final_timestamp = None
        if precedence_timestamp and actual_timestamp:
            if actual_timestamp >= precedence_timestamp:
                final_timestamp = actual_timestamp
                calc_details["method"] = "actual_over_precedence"
                calc_details["source"] = actual_formula
                calc_details["decision_reason"] = f"Actual date ({actual_timestamp.strftime('%Y-%m-%d')}) is later than precedence date ({precedence_timestamp.strftime('%Y-%m-%d')})"
                calc_details["final_choice"] = "actual"
            else:
                final_timestamp = precedence_timestamp
                calc_details["method"] = "precedence_over_actual"
                calc_details["source"] = f"Calculated from dependencies"
                calc_details["decision_reason"] = f"Precedence stage's timestamp ({precedence_timestamp.strftime('%Y-%m-%d')}) is later than actual ({actual_timestamp.strftime('%Y-%m-%d')})"
                calc_details["final_choice"] = "precedence"
        
        elif actual_timestamp:
            final_timestamp = actual_timestamp
            calc_details["method"] = "actual_only"
            calc_details["source"] = actual_formula
            calc_details["decision_reason"] = "Using actual timestamp"
            calc_details["final_choice"] = "actual"
        
        elif precedence_timestamp:
            final_timestamp = precedence_timestamp + timedelta(days=stage.lead_time)
            calc_details["method"] = "precedence_only"
            calc_details["source"] = f"Calculated from dependencies + {stage.lead_time} days"
            calc_details["decision_reason"] = "No actual timestamp available, using precedence calculation"
            calc_details["final_choice"] = "precedence"
        
        # 4. Fallback calculation if no valid timestamp

        # Cache result
        result = (final_timestamp, calc_details)
        self.calculated_adjustments[stage_id] = result
        
        return result
    
    def _extract_actual_field(self, expression: str) -> Optional[str]:
        """
        Extract the 'actual' field from a max() expression
        
        For expressions like "max(actual_field, fallback)", returns "actual_field"
        """
        try:
            tree = ast.parse(expression, mode='eval')
            if isinstance(tree.body, ast.Call) and isinstance(tree.body.func, ast.Name):
                if tree.body.func.id == 'max' and tree.body.args:
                    first_arg = tree.body.args[0]
                    if isinstance(first_arg, ast.Name):
                        return first_arg.id
        except:
            pass
        return None
    
    def calculate_tat(self, po_row: pd.Series) -> Dict[str, Any]:
        """
        Calculate TAT for all stages of a PO
        
        Args:
            po_row: Pandas Series containing PO data
            
        Returns:
            Dictionary with complete TAT calculation results
        """
        # Clear cache for new calculation
        self.calculated_adjustments = {}
        
        result = {
            "po_id": po_row.get('po_razin_id', 'Unknown'),
            "calculation_date": datetime.now().isoformat(),
            "summary": {
                "total_stages": len(self.config.stages),
                "calculated_stages": 0,
                "methods_used": {
                    "actual_only": 0,
                    "precedence_only": 0,
                    "actual_over_precedence": 0,
                    "precedence_over_actual": 0,
                    "fallback": 0,
                    "failed": 0
                }
            },
            "stages": {}
        }
        
        # Calculate each stage
        for stage_id, stage_config in self.config.stages.items():
            timestamp, calc_details = self.calculate_adjusted_timestamp(stage_id, po_row)
            
            # Update summary statistics
            if timestamp:
                result["summary"]["calculated_stages"] += 1
            
            method = calc_details.get("method", "unknown") if isinstance(calc_details, dict) else "legacy"
            if method in result["summary"]["methods_used"]:
                result["summary"]["methods_used"][method] += 1
            
            # Create clean stage result
            stage_result = {
                "name": stage_config.name,
                "timestamp": timestamp.isoformat() if timestamp else None,
                "calculation": self._format_calculation_summary(calc_details, stage_config),
                "process_flow": {
                    "team_owner": stage_config.process_flow.team_owner,
                    "process_type": stage_config.process_flow.process_type,
                    "critical_path": stage_config.process_flow.critical_path,
                    "handoff_points": stage_config.process_flow.handoff_points
                },
                "dependencies": calc_details.get("dependencies", []) if isinstance(calc_details, dict) else []
            }
            
            result["stages"][stage_id] = stage_result
        
        # Calculate completion rate
        result["summary"]["completion_rate"] = round(
            result["summary"]["calculated_stages"] / result["summary"]["total_stages"] * 100, 2
        ) if result["summary"]["total_stages"] > 0 else 0
        
        return result
    
    def _format_calculation_summary(self, calc_details: Dict[str, Any], stage_config: StageConfig) -> Dict[str, Any]:
        """
        Format calculation details into a clean, readable summary
        
        Args:
            calc_details: Raw calculation details
            stage_config: Stage configuration
            
        Returns:
            Clean calculation summary
        """
        if not isinstance(calc_details, dict):
            return {"method": "legacy", "summary": str(calc_details)}
        
        method = calc_details.get("method", "unknown")
        
        summary = {
            "method": method,
            "source": calc_details.get("source"),
            "decision": calc_details.get("decision_reason"),
            "lead_time_days": calc_details.get("lead_time_applied", 0),
            "target_date": calc_details.get("target_date")
        }
        
        # Add method-specific details
        if method == "actual_over_precedence":
            summary.update({
                "actual_date": calc_details.get("actual_value"),
                "precedence_date": calc_details.get("precedence_value"),
                "reason": "Actual timestamp is later than calculated precedence"
            })
        elif method == "precedence_over_actual":
            summary.update({
                "actual_date": calc_details.get("actual_value"),
                "precedence_date": calc_details.get("precedence_value"), 
                "reason": "Calculated precedence is later than actual timestamp"
            })
        elif method == "precedence_only":
            summary.update({
                "precedence_date": calc_details.get("precedence_date"),
                "reason": "No actual timestamp available"
            })
        elif method == "actual_only":
            summary.update({
                "actual_field": calc_details.get("actual_field"),
                "reason": "No dependency chain available"
            })
        elif method == "fallback":
            summary.update({
                "target_date": calc_details.get("target_date"),
                "expression": calc_details.get("source"),
                "reason": "Using fallback calculation"
            })
        
        return summary
    
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

    def export_to_excel(self, df: pd.DataFrame, results: List[Dict[str, Any]], output_file: str):
        """
        Export original data + calculated timestamps to Excel
        
        Args:
            df: Original DataFrame
            results: TAT calculation results
            output_file: Output Excel file path
        """
        # Create a copy of the original dataframe
        export_df = df.copy()
        
        # Add calculated timestamps for each stage
        for result in results:
            if 'stages' not in result:
                continue
                
            po_id = result['po_id']
            po_index = export_df[export_df['po_razin_id'] == po_id].index
            
            if len(po_index) > 0:
                idx = po_index[0]
                
                # Add calculated timestamps
                for stage_id, stage_data in result['stages'].items():
                    # Use stage name instead of ID for column name
                    stage_name = stage_data['name']
                    col_name = f"{stage_name}_Date"
                    timestamp = stage_data['timestamp']
                    # Convert timestamp to date only
                    if timestamp:
                        date = pd.to_datetime(timestamp).date()
                        export_df.loc[idx, col_name] = date
                    else:
                        export_df.loc[idx, col_name] = None
        
        # Save to Excel
        export_df.to_excel(output_file, index=False)
        logger.info(f"Results exported to: {output_file}")


if __name__ == "__main__":
    print("TAT Calculator System - Enhanced with Excel Export")
    print("Usage:")
    print("1. calculator = TATCalculator()")
    print("2. results = calculator.process_batch(df)")
    print("3. calculator.export_to_excel(df, results, 'output.xlsx')")
