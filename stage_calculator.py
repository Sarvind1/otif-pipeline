"""
Stage Calculator Module
=======================

Core calculation logic for individual stages with simplified method-based approach.
Methods: Projected, Actual, Adjusted
Enhanced to support date expressions as preceding stages.
"""

import ast
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any, List
import pandas as pd
from models_config import StageConfig, StagesConfig
from expression_evaluator import ExpressionEvaluator


class StageCalculator:
    """
    Calculates timestamps for individual stages using simplified logic:
    
    - Method: Projected/Actual/Adjusted
    - Target Timestamp: Based on preceding final_timestamp + lead_time
    - Actual Timestamp: From actual_field or preceding actual
    - Final Timestamp: Target (if Projected) or Actual (if Actual/Adjusted)
    - Delay: Target - Actual (only for Actual/Adjusted)
    
    Enhanced to support date expressions as virtual preceding stages.
    """
    
    def __init__(self, config: StagesConfig, expression_evaluator: ExpressionEvaluator):
        self.config = config
        self.expression_evaluator = expression_evaluator
        self.calculated_adjustments: Dict[str, Dict[str, Any]] = {}
        self.expression_evaluator.set_calculated_adjustments(self.calculated_adjustments)

        # Create DataFrame with all combinations and results
        # In the __init__ method, replace the data creation with:
        data = [
            {'input1': 'True', 'input2': 'True', 'result': 'Active'},
            {'input1': 'True', 'input2': 'False', 'result': 'Early'},
            {'input1': 'True', 'input2': 'None', 'result': 'Active'},
            {'input1': 'False', 'input2': 'True', 'result': 'Future'},
            {'input1': 'False', 'input2': 'False', 'result': 'Future'},
            {'input1': 'False', 'input2': 'None', 'result': 'Future'},
            {'input1': 'None', 'input2': 'True', 'result': 'Active'},
            {'input1': 'None', 'input2': 'False', 'result': 'Future'},
            {'input1': 'None', 'input2': 'None', 'result': 'Result_None_None'}
        ]
        self.precedence_map = pd.DataFrame(data)

    
    def calculate_adjusted_timestamp(self, stage_id: str, po_row: pd.Series) -> Dict[str, Any]:
        """Calculate adjusted timestamp for a given stage."""
        print(f"\n--- Calculating stage: {stage_id} ---")
        
        # Return cached result if available
        if stage_id in self.calculated_adjustments:
            print(f"Using cached result for stage: {stage_id}")
            return self.calculated_adjustments[stage_id]
        
        # Validate stage exists in configuration
        if stage_id not in self.config.stages:
            print(f"ERROR: Stage {stage_id} not found in config.")
            error_result = {"method": "error", "reason": f"Stage {stage_id} not found"}
            return error_result
        
        stage = self.config.stages[stage_id]
        print(f"Stage config loaded for {stage_id}: {stage.name}")
        
        # Initialize calculation details structure
        calc_details = self._initialize_calculation_details(stage)
        
        # Process preceding stages (including date expressions)
        self._process_preceding_stages(stage, po_row, calc_details)
        
        # Evaluate lead time from expression
        lead_time_days = self._evaluate_lead_time(stage, po_row)
        calc_details["lead_time_applied"] = lead_time_days
        
        # Evaluate dependencies to get timestamps and status
        prec_actual_timestamps, prec_final_timestamps, status = self.evaluate_dependencies(calc_details["dependencies"])
        calc_details["status"] = status
        
        # Calculate target timestamp
        self._calculate_target_from_precedence(prec_final_timestamps, lead_time_days, calc_details)
        
        # Evaluate actual timestamp from field
        current_actual_timestamp = self._evaluate_actual_timestamp(stage, po_row)
        
        # Determine method and set final timestamp
        self._determine_method_and_final_timestamp(
            current_actual_timestamp, prec_actual_timestamps, calc_details
        )
        
        # Calculate delay if applicable
        self._calculate_delay(stage_id, calc_details)
        
        # Cache and return result
        self.calculated_adjustments[stage_id] = calc_details
        print(f"--- Finished calculation for stage: {stage_id} ---\n")
        print (f"Calculation details: {calc_details}")
        return calc_details
    
    def _initialize_calculation_details(self, stage: StageConfig) -> Dict[str, Any]:
        """Initialize the calculation details dictionary."""
        return {
            "method": None,
            "status": None,
            "target_timestamp": None,
            "actual_timestamp": None,
            "final_timestamp": None,
            "delay": None,
            "lead_time_applied": None,
            "dependencies": [],
            "actual_field": stage.actual_timestamp,
            "calculation_source": None
        }
    
    def _evaluate_lead_time(self, stage: StageConfig, po_row: pd.Series) -> float:
        """Evaluate lead time from stage expression."""
        print(f"Evaluating lead_time expression: {stage.lead_time}")
        lead_time_days, lead_time_debug = self.expression_evaluator.evaluate_expression(
            str(stage.lead_time), po_row
        )
        print(f"Lead time evaluated to: {lead_time_days} (Debug: {lead_time_debug})")
        
        if not isinstance(lead_time_days, (int, float)):
            print(f"Lead time is not a number, defaulting to 0")
            lead_time_days = 0
        
        return lead_time_days
    
    def _process_preceding_stages(
        self, 
        stage: StageConfig, 
        po_row: pd.Series, 
        calc_details: Dict[str, Any]
    ) -> None:
        """Process all preceding stages and date expressions, collecting their timestamps."""
        if not stage.preceding_stage:
            print("No preceding stages defined")
            return
        
        # Evaluate preceding stage expression
        preceding_stage_ids = self._evaluate_preceding_stage_ids(stage, po_row)
        
        # Process each preceding stage or date expression
        for prec_stage_id in preceding_stage_ids:
            prec_stage_id = str(prec_stage_id)

            if prec_stage_id in self.config.stages:
                # Regular stage processing
                details = self.calculate_adjusted_timestamp(prec_stage_id, po_row)
                timestamp = details.get("final_timestamp")
                actual_timestamp = details.get("actual_timestamp")
                target_timestamp = details.get("target_timestamp")

                calc_details["dependencies"].append({
                    "stage_id": prec_stage_id,
                    "stage_name": self.config.stages[prec_stage_id].name,
                    "timestamp": timestamp,  # Already a string or None
                    "actual_timestamp": actual_timestamp,  # Already a string or None
                    "target_timestamp": target_timestamp,  # Already a string or None
                    "method": details.get("method", "unknown"),
                    "stage_type": "actual"
                })

            else:
                # Virtual stage: date expression
                print(f"Processing date expression as virtual stage: {prec_stage_id}")
                evaluated_date = self._process_date_expression(prec_stage_id, po_row)
                
                if evaluated_date:  # Only add if successfully evaluated
                    calc_details["dependencies"].append({
                        "stage_id": prec_stage_id,
                        "stage_name": f"Date Expression: {prec_stage_id}",
                        "timestamp": evaluated_date.isoformat() if isinstance(evaluated_date, datetime) else str(evaluated_date),
                        "stage_type": "virtual",
                    })
    
    def _process_date_expression(
        self, 
        date_expr: str, 
        po_row: pd.Series, 
    ) -> Optional[datetime]:
        """Process a date expression as a virtual preceding stage."""
        try:
            # Evaluate the date expression
            evaluated_date, debug_info = self.expression_evaluator.evaluate_expression(date_expr, po_row)
            print(f"Date expression '{date_expr}' evaluated to: {evaluated_date} (Debug: {debug_info})")
            
            if not evaluated_date or not isinstance(evaluated_date, datetime):
                print(f"Date expression '{date_expr}' did not evaluate to a valid date")
                return None
            
            # Determine if this is actual or projected based on current date
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            method = "Actual" if today >= evaluated_date else "Virtual"
            
            print(f"Date expression '{date_expr}': {evaluated_date} -> Method: {method} (Today: {today})")
            
            return evaluated_date
            
        except Exception as e:
            print(f"ERROR processing date expression '{date_expr}': {e}")
            return None
    
    def _evaluate_preceding_stage_ids(self, stage: StageConfig, po_row: pd.Series) -> List[str]:
        """Evaluate the preceding stage expression to get stage IDs."""
        try:
            print(f"Evaluating preceding stage expression for {stage.name}: {stage.preceding_stage}")
            result = self.expression_evaluator._eval_node(
                ast.parse(stage.preceding_stage, mode='eval').body, po_row
            )
            preceding_stage_ids = result if isinstance(result, list) else []
            print(f"Preceding stage IDs: {preceding_stage_ids}")
            return preceding_stage_ids
        except Exception as e:
            print(f"ERROR evaluating preceding stage for {stage.name}: {e}")
            return []
    
    def _parse_timestamp_from_details(self, stage_id: str, details: Dict[str, Any], ts_key: str) -> Optional[datetime]:
        """Parse timestamp from stage details."""
        try:
            ts_value = details.get(ts_key)
            if not ts_value:
                return None
            
            if isinstance(ts_value, datetime):
                return ts_value
            elif isinstance(ts_value, str):
                parsed_ts = datetime.fromisoformat(ts_value)
                print(f"Parsed timestamp for {stage_id} ({ts_key}): {parsed_ts}")
                return parsed_ts
            else:
                print(f"Invalid timestamp type for {stage_id} ({ts_key}): {type(ts_value)}")
                return None
                
        except Exception as e:
            print(f"Invalid timestamp in stage {stage_id} ({ts_key}): {e}")
            return None
    
    def evaluate_dependencies(self, dependencies: List[Dict[str, Any]]) -> Tuple[List[datetime], List[datetime], str]:
        """Evaluate dependencies to extract actual and target timestamps."""
        actual_timestamps = []
        final_timestamps = []
        is_done = []
        is_happened = []
        
        for dep in dependencies:
            print(f"Evaluating dependency: {dep}")
            
            # Parse final timestamp - only add if valid
            if dep.get("timestamp"):
                try:
                    if isinstance(dep["timestamp"], str):
                        final_ts = datetime.fromisoformat(dep["timestamp"])
                    elif isinstance(dep["timestamp"], datetime):
                        final_ts = dep["timestamp"]
                    else:
                        final_ts = None
                    
                    if final_ts:
                        final_timestamps.append(final_ts)
                except Exception as e:
                    print(f"Failed to parse final timestamp for {dep['stage_id']}: {e}")
            
            if dep["stage_type"] == "actual":
                # Parse actual timestamp
                if dep.get("actual_timestamp"):
                    try:
                        if isinstance(dep["actual_timestamp"], str):
                            actual_ts = datetime.fromisoformat(dep["actual_timestamp"])
                        elif isinstance(dep["actual_timestamp"], datetime):
                            actual_ts = dep["actual_timestamp"]
                        else:
                            actual_ts = None
                        
                        if actual_ts:
                            actual_timestamps.append(actual_ts)
                            is_done.append(True)
                        else:
                            is_done.append(False)
                    except Exception as e:
                        print(f"Failed to parse actual timestamp for {dep['stage_id']}: {e}")
                        is_done.append(False)
                else:
                    is_done.append(False)
                    
            elif dep["stage_type"] == "virtual":
                if dep.get("timestamp"):
                    is_happened.append(True)
                else:
                    is_happened.append(False)
        
        # Determine status based on precedence logic
        input1 = str(all(is_done)) if is_done else 'None'               #handling empties
        input2 = str(all(is_happened)) if is_happened else 'None'       #handling empties
        
        try:
            status_matches = self.precedence_map[
                (self.precedence_map['input1'] == input1) & (self.precedence_map['input2'] == input2)
            ]['result'].values
            # print (f"Determined status: {status_matches, input1, input2}")
            # print ("precedence map", self.precedence_map)
            # print ("map_view", self.precedence_map['input2']==input2)
            status = status_matches[0] if len(status_matches) > 0 else "Unknown"
        except Exception as e:
            print(f"Error determining status: {e}")
            status = "Unknown"

        return actual_timestamps, final_timestamps, status

    
    def _calculate_target_from_precedence(
        self, 
        preceding_final_timestamps: List[datetime], 
        lead_time_days: float, 
        calc_details: Dict[str, Any]
    ) -> None:
        """Calculate target timestamp from preceding timestamps."""
        print(f"Preceding final timestamps: {preceding_final_timestamps}")
        
        if not preceding_final_timestamps:
            print("WARNING: No preceding final timestamps available, cannot calculate target")
            calc_details["target_timestamp"] = None
            calc_details["calculation_source"] = "no_precedence"
            return
        
        # Filter out None values and get max
        valid_timestamps = [ts for ts in preceding_final_timestamps if ts is not None]
        if not valid_timestamps:
            print("WARNING: No valid preceding timestamps found, cannot calculate target")
            calc_details["target_timestamp"] = None
            calc_details["calculation_source"] = "no_valid_precedence"
            return
        
        base_timestamp = max(valid_timestamps)
        print(f"Max preceding final timestamp: {base_timestamp}")
        calc_details["target_timestamp"] = (base_timestamp + timedelta(days=lead_time_days)).isoformat()
        calc_details["calculation_source"] = "precedence_based"
        print(f"Target timestamp (precedence based): {calc_details['target_timestamp']}")
    
    def _evaluate_actual_timestamp(self, stage: StageConfig, po_row: pd.Series) -> Optional[datetime]:
        """Evaluate actual timestamp from stage configuration."""
        if not stage.actual_timestamp:
            return None
        
        print(f"Evaluating actual timestamp for {stage.name}: {stage.actual_timestamp}")
        actual_result, actual_debug = self.expression_evaluator.evaluate_expression(
            stage.actual_timestamp, po_row
        )
        print(f"Actual timestamp evaluated to: {actual_result} (Debug: {actual_debug})")
        
        if actual_result:
            print(f"Actual timestamp: {actual_result}")
            return actual_result
        
        return None
    
    def _determine_method_and_final_timestamp(
        self, 
        current_actual_timestamp: Optional[datetime], 
        preceding_actual_timestamps: List[datetime], 
        calc_details: Dict[str, Any]
    ) -> None:
        """Determine calculation method and set final timestamp."""
        if current_actual_timestamp:
            self._handle_actual_timestamp_case(
                current_actual_timestamp, preceding_actual_timestamps, calc_details
            )
        else:
            self._handle_projected_timestamp_case(preceding_actual_timestamps, calc_details)
    
    def _handle_actual_timestamp_case(
        self, 
        current_actual_timestamp: datetime, 
        preceding_actual_timestamps: List[datetime], 
        calc_details: Dict[str, Any]
    ) -> None:
        """Handle case where actual timestamp exists."""
        max_preceding_actual = max(preceding_actual_timestamps) if preceding_actual_timestamps else None
        
        if (max_preceding_actual) and (max_preceding_actual > current_actual_timestamp):
            # Adjusted method - preceding actual overrides current
            calc_details["method"] = "Adjusted"
            calc_details["actual_timestamp"] = max_preceding_actual.isoformat()
            calc_details["final_timestamp"] = max_preceding_actual.isoformat()
            calc_details["calculation_source"] = "actual_from_precedence"
            print("Method: Adjusted (preceding actual overrides current)")
        else:
            # Actual method - use current actual
            calc_details["method"] = "Actual"
            calc_details["actual_timestamp"] = current_actual_timestamp.isoformat()
            calc_details["final_timestamp"] = current_actual_timestamp.isoformat()
            calc_details["calculation_source"] = "actual_from_field"
            print("Method: Actual")
    
    def _handle_projected_timestamp_case(
        self, 
        preceding_actual_timestamps: List[datetime], 
        calc_details: Dict[str, Any]
    ) -> None:
        """Handle case where no actual timestamp exists (projected)."""
        calc_details["method"] = "Projected"
        calc_details["final_timestamp"] = calc_details["target_timestamp"]
        calc_details["calculation_source"] = (calc_details["calculation_source"] or "") + "_target"
        
        print("Method: Projected (no actuals available)")
    
    def _calculate_delay(self, stage_id: str, calc_details: Dict[str, Any]) -> None:
        """Calculate delay between target and actual timestamps."""
        method = calc_details.get("method")
        target_ts = calc_details.get("target_timestamp")
        actual_ts = calc_details.get("actual_timestamp")
        
        # Calculate delay for Actual/Adjusted methods
        if method in ["Actual", "Adjusted"] and target_ts and actual_ts:
            try:
                target_dt = datetime.fromisoformat(target_ts)
                actual_dt = datetime.fromisoformat(actual_ts)
                delay_days = (actual_dt - target_dt).days
                calc_details["delay"] = delay_days
                calc_details["status"] = "Historic"
                print(f"Delay: {delay_days} days")
            except Exception as e:
                print(f"ERROR calculating delay for {stage_id}: {e}")
        elif method == "Projected" and target_ts and target_ts< datetime.now().isoformat() and calc_details["actual_field"]=="":
            # For projected stages past target date, set delay as negative days from today
            try:
                calc_details["status"] = "Historic_Virtual"
                print(f"Projected stage past target date, Delay: {delay_days} days")
            except Exception as e:
                print(f"ERROR calculating projected delay for {stage_id}: {e}")
    
    def _parse_final_timestamp(self, stage_id: str, calc_details: Dict[str, Any]) -> Optional[datetime]:
        """Parse the final timestamp from calculation details."""
        if not calc_details["final_timestamp"]:
            return None
        
        try:
            final_timestamp = datetime.fromisoformat(calc_details["final_timestamp"])
            print(f"Final timestamp: {final_timestamp}")
            return final_timestamp
        except Exception as e:
            print(f"ERROR parsing final timestamp for {stage_id}: {e}")
            return None
    
    def reset_cache(self):
        """Reset the calculation cache."""
        print("Resetting stage calculation cache")
        self.calculated_adjustments = {}
        self.expression_evaluator.set_calculated_adjustments(self.calculated_adjustments)