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
        self.calculated_adjustments: Dict[str, Tuple[Optional[datetime], Dict[str, Any]]] = {}
        self.expression_evaluator.set_calculated_adjustments(self.calculated_adjustments)
    
    def calculate_adjusted_timestamp(self, stage_id: str, po_row: pd.Series) -> Tuple[Optional[datetime], Dict[str, Any]]:
        """Calculate adjusted timestamp for a given stage."""
        print(f"\n--- Calculating stage: {stage_id} ---")
        
        # Return cached result if available
        if stage_id in self.calculated_adjustments:
            print(f"Using cached result for stage: {stage_id}")
            return self.calculated_adjustments[stage_id]
        
        # Validate stage exists in configuration
        if stage_id not in self.config.stages:
            print(f"ERROR: Stage {stage_id} not found in config.")
            return None, {"method": "error", "reason": f"Stage {stage_id} not found"}
        
        stage = self.config.stages[stage_id]
        print(f"Stage config loaded for {stage_id}: {stage.name}")
        
        # Initialize calculation details structure
        calc_details = self._initialize_calculation_details(stage)
        
        # Evaluate lead time from expression
        lead_time_days = self._evaluate_lead_time(stage, po_row)
        calc_details["lead_time_applied"] = lead_time_days
        
        # Process preceding stages (including date expressions)
        (preceding_final_timestamps, 
         preceding_actual_timestamps, 
         has_projected_precedence, 
         preceding_stage_ids) = self._process_preceding_stages(stage, po_row, calc_details)
        
        # Calculate target timestamp
        self._calculate_target_timestamp(
            stage, po_row, preceding_final_timestamps, 
            lead_time_days, calc_details
        )
        
        # Set precedence method
        calc_details["precedence_method"] = self._determine_precedence_method(
            preceding_stage_ids, has_projected_precedence
        )
        
        # Evaluate actual timestamp from field
        current_actual_timestamp = self._evaluate_actual_timestamp(stage, po_row)
        
        # print( "temp debug : actual timestamp = " , calc_details["actual_timestamp"]) 
        
        # Determine method and set final timestamp
        self._determine_method_and_final_timestamp(
            current_actual_timestamp, preceding_actual_timestamps, calc_details
        )
        
        # Calculate delay if applicable
        self._calculate_delay(stage_id, calc_details)
        
        # Parse and return final timestamp
        final_timestamp = self._parse_final_timestamp(stage_id, calc_details)

        
        # Cache and return result
        result = (final_timestamp, calc_details)
        self.calculated_adjustments[stage_id] = result
        # print( "temp debug 2 : actual timestamp = " , calc_details["actual_timestamp"]) 

        print(f"--- Finished calculation for stage: {stage_id} ---\n")
        return result
    
    def _initialize_calculation_details(self, stage: StageConfig) -> Dict[str, Any]:
        """Initialize the calculation details dictionary."""
        return {
            "method": None,
            "target_timestamp": None,
            "actual_timestamp": None,
            "final_timestamp": None,
            "delay": None,
            "lead_time_applied": None,
            "dependencies": [],
            "actual_field": stage.actual_timestamp,
            "precedence_method": None,
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
    ) -> Tuple[List[datetime], List[datetime], bool, List[str]]:
        """Process all preceding stages and date expressions, collecting their timestamps."""
        preceding_final_timestamps = []
        preceding_actual_timestamps = []
        has_projected_precedence = False
        preceding_stage_ids = []
        
        if not stage.preceding_stage:
            return preceding_final_timestamps, preceding_actual_timestamps, has_projected_precedence, preceding_stage_ids
        
        # Evaluate preceding stage expression
        preceding_stage_ids = self._evaluate_preceding_stage_ids(stage, po_row)
        
        # Process each preceding stage or date expression
        for prec_stage_id in preceding_stage_ids:
            prec_stage_id = str(prec_stage_id)
            
            if prec_stage_id in self.config.stages:
                # Regular stage processing
                self._process_regular_stage(prec_stage_id, po_row, calc_details, 
                                          preceding_final_timestamps, preceding_actual_timestamps, 
                                          has_projected_precedence)
            else:
                # Check if it's a date expression
                print(f"Processing date expression as virtual stage: {prec_stage_id}")
                has_projected_precedence = self._process_date_expression(
                    prec_stage_id, po_row, calc_details, 
                    preceding_final_timestamps, preceding_actual_timestamps, 
                    has_projected_precedence
                ) or has_projected_precedence
                
        return preceding_final_timestamps, preceding_actual_timestamps, has_projected_precedence, preceding_stage_ids
    
    def _process_regular_stage(
        self, 
        prec_stage_id: str, 
        po_row: pd.Series, 
        calc_details: Dict[str, Any],
        preceding_final_timestamps: List[datetime],
        preceding_actual_timestamps: List[datetime],
        has_projected_precedence: bool
    ) -> bool:
        """Process a regular stage as precedence."""
        print(f"Calculating preceding stage {prec_stage_id}")
        prec_timestamp, prec_details = self.calculate_adjusted_timestamp(prec_stage_id, po_row)
        
        # Collect final timestamps
        if prec_timestamp:
            preceding_final_timestamps.append(prec_timestamp)
            print(f"Preceding final timestamp for {prec_stage_id}: {prec_timestamp}")
        
        # Collect actual timestamps
        if prec_details.get("actual_timestamp"):
            actual_ts = self._parse_timestamp_from_details(prec_stage_id, prec_details)
            if actual_ts:
                preceding_actual_timestamps.append(actual_ts)
        
        # Check for projected precedence
        if prec_details.get("method") == "Projected":
            has_projected_precedence = True
        
        # Add to dependencies
        self._add_dependency_to_calc_details(prec_stage_id, prec_timestamp, prec_details, calc_details)
        
        return has_projected_precedence
    
    def _is_date_expression(self, expression: str) -> bool:
        """Check if a string looks like a date expression (contains date field operations)."""
        # Simple heuristic: contains common date field patterns and arithmetic
        date_indicators = ['-', '+', '_date', '_ts', 'final_prd', 'po_created', 'supplier_confirmation']
        arithmetic_indicators = ['-', '+']
        
        has_date_field = any(indicator in expression.lower() for indicator in date_indicators)
        has_arithmetic = any(op in expression for op in arithmetic_indicators)
        
        return has_date_field and has_arithmetic
    
    def _process_date_expression(
        self, 
        date_expr: str, 
        po_row: pd.Series, 
        calc_details: Dict[str, Any],
        preceding_final_timestamps: List[datetime],
        preceding_actual_timestamps: List[datetime],
        has_projected_precedence: bool
    ) -> bool:
        """Process a date expression as a virtual preceding stage."""
        try:
            # Evaluate the date expression
            evaluated_date, debug_info = self.expression_evaluator.evaluate_expression(date_expr, po_row)
            print(f"Date expression '{date_expr}' evaluated to: {evaluated_date} (Debug: {debug_info})")
            
            if not evaluated_date or not isinstance(evaluated_date, datetime):
                print(f"Date expression '{date_expr}' did not evaluate to a valid date")
                return has_projected_precedence
            
            # Determine if this is actual or projected based on current date
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            is_actual = today >= evaluated_date
            method = "Actual" if is_actual else "Projected"
            
            print(f"Date expression '{date_expr}': {evaluated_date} -> Method: {method} (Today: {today})")
            
            # Add to timestamps
            preceding_final_timestamps.append(evaluated_date)
            if is_actual:
                preceding_actual_timestamps.append(evaluated_date)
            else:
                has_projected_precedence = True
            
            # Create virtual stage details for dependency tracking
            virtual_stage_details = {
                "method": method,
                "target_timestamp": evaluated_date.isoformat(),
                "actual_timestamp": evaluated_date.isoformat() if is_actual else None,
                "final_timestamp": evaluated_date.isoformat(),
                "delay": 0 if is_actual else None,
                "virtual_stage": True,
                "expression": date_expr
            }
            
            # Add to dependencies
            calc_details["dependencies"].append({
                "stage_id": date_expr,
                "stage_name": f"Date Expression: {date_expr}",
                "timestamp": evaluated_date.isoformat(),
                "method": method,
                "virtual_stage": True
            })
            
            return has_projected_precedence
            
        except Exception as e:
            print(f"ERROR processing date expression '{date_expr}': {e}")
            return has_projected_precedence
    
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
    
    def _parse_timestamp_from_details(self, stage_id: str, details: Dict[str, Any]) -> Optional[datetime]:
        """Parse timestamp from stage details."""
        try:
            actual_ts = datetime.fromisoformat(details["actual_timestamp"])
            print(f"Preceding actual timestamp for {stage_id}: {actual_ts}")
            return actual_ts
        except Exception as e:
            print(f"Invalid actual timestamp in stage {stage_id}: {e}")
            return None
    
    def _add_dependency_to_calc_details(
        self, 
        stage_id: str, 
        timestamp: Optional[datetime], 
        details: Dict[str, Any], 
        calc_details: Dict[str, Any]
    ) -> None:
        """Add dependency information to calculation details."""
        calc_details["dependencies"].append({
            "stage_id": stage_id,
            "stage_name": self.config.stages[stage_id].name,
            "timestamp": timestamp.isoformat() if timestamp else None,
            "method": details.get("method", "unknown")
        })
    
    def _calculate_target_timestamp(
        self, 
        stage: StageConfig, 
        po_row: pd.Series, 
        preceding_final_timestamps: List[datetime], 
        lead_time_days: float, 
        calc_details: Dict[str, Any]
    ) -> None:
        """Calculate the target timestamp based on precedence or fallback."""
        if preceding_final_timestamps:
            self._calculate_target_from_precedence(preceding_final_timestamps, lead_time_days, calc_details)
        else:
            self._calculate_target_from_fallback(stage, po_row, lead_time_days, calc_details)
    
    def _calculate_target_from_precedence(
        self, 
        preceding_final_timestamps: List[datetime], 
        lead_time_days: float, 
        calc_details: Dict[str, Any]
    ) -> None:
        """Calculate target timestamp from preceding timestamps."""
        base_timestamp = max(preceding_final_timestamps)
        print(f"Max preceding final timestamp: {base_timestamp}")
        calc_details["target_timestamp"] = (base_timestamp + timedelta(days=lead_time_days)).isoformat()
        calc_details["calculation_source"] = "precedence_based"
        print(f"Target timestamp (precedence based): {calc_details['target_timestamp']}")
    
    def _calculate_target_from_fallback(
        self, 
        stage: StageConfig, 
        po_row: pd.Series, 
        lead_time_days: float, 
        calc_details: Dict[str, Any]
    ) -> None:
        """Calculate target timestamp from fallback expression."""
        print("No valid preceding timestamps, evaluating fallback expression")
        try:
            fallback_result, fallback_debug = self.expression_evaluator.evaluate_expression(
                stage.fallback_calculation.expression, po_row
            )
            print(f"Fallback expression evaluated to: {fallback_result} (Debug: {fallback_debug})")
            
            if fallback_result:
                calc_details["target_timestamp"] = (fallback_result + timedelta(days=lead_time_days)).isoformat()
                calc_details["calculation_source"] = "fallback_based"
                print(f"Target timestamp (fallback): {calc_details['target_timestamp']}")
        except Exception as e:
            print(f"ERROR in fallback evaluation for {stage.name}: {e}")
    
    def _determine_precedence_method(self, preceding_stage_ids: List[str], has_projected_precedence: bool) -> str:
        """Determine the precedence method based on preceding stages."""
        if preceding_stage_ids:
            return "Projected" if has_projected_precedence else "Actual/Adjusted"
        else:
            return "no precedence"
    
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
        
        # if preceding_actual_timestamps:
        #     calc_details["actual_timestamp"] = max(preceding_actual_timestamps).isoformat() #why this line?
        
        print("Method: Projected (no actuals available)")
    
    def _calculate_delay(self, stage_id: str, calc_details: Dict[str, Any]) -> None:
        """Calculate delay between target and actual timestamps."""
        if (calc_details["method"] not in ["Actual", "Adjusted"] or 
            not calc_details["target_timestamp"] or 
            not calc_details["actual_timestamp"]):
            return
        
        try:
            target_dt = datetime.fromisoformat(calc_details["target_timestamp"])
            actual_dt = datetime.fromisoformat(calc_details["actual_timestamp"])
            delay_days = (actual_dt - target_dt).days
            calc_details["delay"] = delay_days
            print(f"Delay: {delay_days} days")
        except Exception as e:
            print(f"ERROR calculating delay for {stage_id}: {e}")
    
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