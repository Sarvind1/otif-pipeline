# TAT Calculator - Commits Guide

## Latest Changes (January 2025)

### Refactoring: Simplified TAT Calculation Logic
**Date**: 2025-01-08
**Branch**: main

#### Changes Made:
1. **Modified `stage_calculator.py`**:
   - Implemented new simplified calculation logic with Method (Projected/Actual/Adjusted)
   - Added target_timestamp calculation: max(preceding.final_timestamp) + lead_time
   - Added actual_timestamp handling with precedence comparison
   - Added final_timestamp: target (if Projected) or actual (if Actual/Adjusted)
   - Integrated delay calculation: target - actual (only for Actual/Adjusted)
   - Added calculation_source field to track how final timestamp was determined:
     * "precedence_based_target" - from preceding stages' target
     * "fallback_based_target" - from fallback when no precedence
     * "actual_from_field" - from actual field in data
     * "actual_from_precedence" - from preceding stage's actual (Adjusted method)
   - Removed complex precedence logic

2. **Deleted `delay_calculator.py`**:
   - Functionality integrated into stage_calculator.py
   - Delay calculation now happens inline during stage calculation

3. **Modified `tat_processor.py`**:
   - Updated to handle new calculation structure
   - Implemented 7-tab Excel export format:
     * Method tab: Shows Projected/Actual/Adjusted for each stage
     * Actual_Timestamps tab: Actual timestamps from data
     * Target_Timestamps tab: Calculated target timestamps
     * Final_Timestamps tab: Final timestamps used
     * Delay tab: Delay in days (only for Actual/Adjusted)
     * Precedence_Method tab: Shows if preceding stages were Projected or Actual/Adjusted
     * Calculation_Source tab: Shows how final timestamp was calculated
   - Removed separate delay processing logic
   - Simplified JSON output structure

4. **Modified `tat_calculator_main.py`**:
   - Removed delay calculator imports and references
   - Simplified main orchestration logic
   - Updated documentation to reflect new approach

5. **Modified `run_tat_calculation.py`**:
   - Fixed to use new export_stage_level_excel method
   - Removed references to deleted methods

#### Key Logic Changes:

**Method Determination:**
- `Projected`: When no actual_field exists
- `Actual`: When actual_field exists and current_actual >= preceding_actual
- `Adjusted`: When actual_field exists but preceding_actual > current_actual (use preceding)

**Timestamp Calculations:**
- `target_timestamp` = max(preceding.final_timestamp) + lead_time
- `actual_timestamp` = current_actual or preceding_actual (based on method)
- `final_timestamp` = target (if Projected) or actual (if Actual/Adjusted)
- `delay` = target - actual (only calculated for Actual/Adjusted methods)

**Calculation Source Tracking:**
- Helps debug by showing exactly how each final timestamp was determined
- Tracks whether it came from precedence, fallback, actual field, or adjusted from precedence

**Fallback Handling:**
- Fallback calculations remain for stages without preceding stages
- New method determination applied to fallback results

#### Output Changes:
- Excel export now has 7 tabs instead of 5
- JSON structure includes precedence_method and calculation_source fields
- Better debugging capabilities with calculation source tracking

#### Files Affected:
- `stage_calculator.py` - Modified
- `tat_processor.py` - Modified  
- `tat_calculator_main.py` - Modified
- `run_tat_calculation.py` - Modified
- `delay_calculator.py` - Deleted
- `commits_guide.md` - Updated
