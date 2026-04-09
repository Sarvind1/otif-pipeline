# TAT Calculator Project Brain 🧠
## Architecture & Developer Guide

> **Purpose**: A comprehensive architectural guide and documentation for the TAT (Turnaround Time) Calculator system. This document serves as the single source of truth for understanding, maintaining, and extending the codebase.

---

## 📋 Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture & Design Principles](#architecture--design-principles)
3. [Modular Structure](#modular-structure)
4. [Core Components Deep Dive](#core-components-deep-dive)
5. [Data Flow & Processing Logic](#data-flow--processing-logic)
6. [Configuration System](#configuration-system)
7. [Expression Engine](#expression-engine)
8. [Developer Quick Reference](#developer-quick-reference)
9. [Common Patterns & Best Practices](#common-patterns--best-practices)
10. [Troubleshooting Guide](#troubleshooting-guide)
11. [Extension Points](#extension-points)

---

## 🎯 Project Overview

### What is TAT Calculator?
The TAT Calculator is a Python-based system that automates the calculation of turnaround times for Purchase Order (PO) processing across 31 distinct workflow stages. It replaces complex, unmaintainable Excel formulas with a configuration-driven, transparent calculation engine.

### Key Business Value
- **Replaces 31 complex Excel formulas** with maintainable Python code
- **Provides audit trails** showing exactly how each timestamp was calculated
- **Identifies bottlenecks** and delays in the PO processing pipeline
- **Scales efficiently** from single PO to thousands of POs

### Core Problem Solved
Excel-based TAT calculations were:
- Impossible to debug (nested formulas 10+ levels deep)
- Difficult to modify (changing one formula could break others)
- Lacking transparency (no explanation of calculation logic)
- Error-prone (manual formula dragging, cell references)

---

## 🏗️ Architecture & Design Principles

### High-Level Architecture
```
┌─────────────────────┐     ┌────────────────────┐     ┌──────────────────┐
│   Excel Input       │────▶│   TAT Calculator   │────▶│  JSON/Excel      │
│  (ts_big.xlsx)      │     │   (Core Engine)    │     │   Outputs        │
└─────────────────────┘     └────────────────────┘     └──────────────────┘
                                     │
                                     ▼
                            ┌────────────────────┐
                            │  stages_config.json │
                            │  (Business Logic)   │
                            └────────────────────┘
```

### Design Principles

1. **Configuration-Driven Design**
   - All business logic externalized to `stages_config.json`
   - No hardcoded stage logic in Python code
   - Easy to modify without touching code

2. **Dependency Graph (DAG)**
   - Stages form a Directed Acyclic Graph
   - Automatic dependency resolution
   - Cycle detection prevents infinite loops

3. **Memoization Pattern**
   - Each stage calculated only once per PO
   - Results cached in `calculated_adjustments`
   - Significant performance improvement for complex dependencies

4. **Expression Evaluation**
   - Dynamic expression parsing using Python AST
   - Safe evaluation (no `eval()` security risks)
   - Extensible function library (`max`, `add_days`, `cond`)

5. **Fail-Safe Calculation Hierarchy**
   ```
   1. Try precedence-based calculation (from dependencies)
   2. Compare with actual timestamp if available
   3. Fall back to expression-based calculation
   4. Provide clear reasoning for each decision
   ```

---

## 📦 Modular Structure

The codebase has been refactored into 4 clean modules:

### 1. **models_config.py** - Data Models & Configuration
- Pydantic models for data validation
- Configuration loading and validation
- Circular dependency detection

### 2. **expression_evaluator.py** - Expression Engine
- Dynamic expression evaluation
- Custom functions (max, add_days, cond)
- Date parsing utilities
- AST-based safe evaluation

### 3. **stage_calculator.py** - Core Stage Calculator
- Individual stage timestamp calculation
- Precedence vs actual logic
- Memoization implementation
- Dependency resolution

### 4. **tat_processor.py** - TAT Processing & Export
- Orchestrates calculations across all stages
- Batch processing capabilities
- Result formatting
- Excel export functionality

### 5. **tat_calculator_main.py** - Main Entry Point
- Coordinates all modules
- Provides backward compatibility
- Single interface for all operations

---

## 🔧 Core Components Deep Dive

### 1. TATCalculator Class (`tat_calculator_main.py`)

The main coordinator that initializes and manages all sub-modules.

```python
class TATCalculator:
    def __init__(self, config_path):
        # Load configuration
        self.config = load_config(config_path)
        validate_config(self.config)
        
        # Initialize components
        self.expression_evaluator = ExpressionEvaluator()
        self.stage_calculator = StageCalculator(self.config, self.expression_evaluator)
        self.tat_processor = TATProcessor(self.config, self.stage_calculator)
```

### 2. ExpressionEvaluator (`expression_evaluator.py`)

Handles all dynamic expression evaluation:

```python
# Supported expression types:
- Field references: "po_created_date"
- Functions: "max(date1, date2)", "add_days(date, 5)"
- Conditionals: "cond(pi_applicable==1, ['5'], ['2'])"
- Complex nested: "max(field1, add_days(field2, 3))"
```

### 3. StageCalculator (`stage_calculator.py`)

Core calculation logic with three-tier priority:

```python
# Priority hierarchy:
1. Precedence-based: Dependencies + lead time
2. Actual timestamp: From PO data
3. Fallback: Expression evaluation
```

### 4. TATProcessor (`tat_processor.py`)

Handles batch operations and output:
- Orchestrates calculations for all stages
- Aggregates results with statistics
- Exports to Excel with calculated dates

---

## 🔄 Data Flow & Processing Logic

### Stage Calculation Flow

```mermaid
graph TD
    A[Start Stage Calculation] --> B{Already Calculated?}
    B -->|Yes| C[Return Cached Result]
    B -->|No| D[Calculate Precedence]
    D --> E[Get Actual Timestamp]
    E --> F{Both Available?}
    F -->|Yes| G[Take max(precedence, actual)]
    F -->|No| H{Precedence Only?}
    H -->|Yes| I[Use Precedence + Lead Time]
    H -->|No| J{Actual Only?}
    J -->|Yes| K[Use Actual]
    J -->|No| L[Evaluate Fallback Expression]
    G --> M[Apply Lead Time]
    I --> M
    K --> M
    L --> M
    M --> N[Cache & Return Result]
```

### Example: Stage 8 Calculation

```json
Stage 8 Dependencies:
- preceding_stage: "cond(pi_applicable==1, ['5'], ['2'])"
- actual_timestamp: "receive_first_prd_date"
- fallback_calculation: "max(receive_first_prd_date, add_days(pi_payment_date, 2))"
- lead_time: 2

Calculation Process:
1. Check if pi_applicable==1
   - If true: depends on Stage 5
   - If false: depends on Stage 2
2. Calculate precedence from chosen dependency
3. Get actual timestamp (receive_first_prd_date)
4. Compare and choose latest
5. Add 2 days lead time
```

---

## ⚙️ Configuration System

### Stage Configuration Structure

Each stage in `stages_config.json` contains:

```json
{
  "8": {
    "name": "08. PRD Pending",
    "actual_timestamp": "receive_first_prd_date",
    "preceding_stage": "cond(pi_applicable==1, ['5'], ['2'])",
    "process_flow": {
      "critical_path": false,
      "parallel_processes": ["6", "7"],
      "process_type": "confirmation",
      "team_owner": "Finance"
    },
    "fallback_calculation": {
      "expression": "max(receive_first_prd_date, add_days(pi_payment_date, 2))"
    },
    "lead_time": 2
  }
}
```

### Key Configuration Patterns

1. **Simple Linear Dependency**
   ```json
   "preceding_stage": "['1']"
   ```

2. **Conditional Dependencies**
   ```json
   "preceding_stage": "cond(pi_applicable==1, ['5'], ['2'])"
   ```

3. **Multiple Dependencies**
   ```json
   "preceding_stage": "['16', '17']"  // Takes max of both
   ```

4. **Complex Fallback Expressions**
   ```json
   "expression": "max(actual_field, add_days(other_field, 5), stage_8+plt-21)"
   ```

---

## 🧮 Expression Engine

### Supported Functions

1. **`max(arg1, arg2, ...)`**
   - Returns latest datetime from arguments
   - Ignores None values
   - Example: `max(date1, date2, date3)`

2. **`add_days(date, days)`**
   - Adds specified days to a date
   - Example: `add_days(po_created_date, 5)`

3. **`cond(condition, true_value, false_value)`**
   - Conditional evaluation (like Excel IF)
   - Lazy evaluation (only evaluates chosen branch)
   - Example: `cond(pi_applicable==1, ['5'], ['2'])`

### Expression Evaluation Process

```python
# AST Node Types Handled:
- ast.Name: Field references (e.g., "po_created_date")
- ast.Call: Function calls (e.g., "max(...)")
- ast.Constant: Literal values (e.g., 5)
- ast.Compare: Comparisons (e.g., "pi_applicable==1")
- ast.BinOp: Binary operations (e.g., "stage_8+plt-21")
- ast.List: List literals (e.g., "['5']")
```

### Adding New Functions

To add a new function (e.g., `min`):

```python
# In _eval_node method in expression_evaluator.py:
elif func_name == 'min':
    valid_dates = [arg for arg in args if isinstance(arg, datetime)]
    return min(valid_dates) if valid_dates else None
```

---

## 🚀 Developer Quick Reference

### Setting Up Development Environment

```bash
# Clone repository
git clone https://github.com/Sarvind1/tat-calculator.git
cd tat-calculator

# Install dependencies
pip install -r requirements.txt

# Run tests on sample data
python run_tat_calculation.py
```

### Common Development Tasks

#### 1. Adding a New Stage
```json
// In stages_config.json:
"32": {
  "name": "32. New Stage Name",
  "actual_timestamp": "new_stage_date_field",
  "preceding_stage": "['31']",
  "process_flow": {
    "critical_path": true,
    "parallel_processes": [],
    "process_type": "new_type",
    "team_owner": "TeamName"
  },
  "fallback_calculation": {
    "expression": "add_days(stage_31, 3)"
  },
  "lead_time": 3
}
```

#### 2. Modifying Stage Dependencies
```json
// Change from simple to conditional:
"preceding_stage": "['5']"  // Before
"preceding_stage": "cond(some_flag==1, ['5'], ['3'])"  // After
```

#### 3. Debugging a Calculation
```python
# Enable detailed logging:
import logging
logging.basicConfig(level=logging.DEBUG)

# Add debug prints in stage_calculator.py:
print(f"Stage {stage_id}: precedence={precedence_timestamp}, actual={actual_timestamp}")
```

### Key Files Reference

| File | Purpose | Key Functions |
|------|---------|---------------|
| `tat_calculator_main.py` | Main entry point | Coordinates all modules |
| `models_config.py` | Data models & config loading | `load_config()`, `validate_config()` |
| `expression_evaluator.py` | Expression evaluation engine | `evaluate_expression()`, `_eval_node()` |
| `stage_calculator.py` | Core calculation logic | `calculate_adjusted_timestamp()` |
| `tat_processor.py` | Batch processing & export | `calculate_tat()`, `export_to_excel()` |
| `stages_config.json` | Business logic configuration | All 31 stage definitions |
| `run_tat_calculation.py` | Runner script | `run_calculations()` |

---

## 🎨 Common Patterns & Best Practices

### 1. Configuration Before Code
- Always try to solve problems in `stages_config.json` first
- Only modify Python code for new features, not business logic changes

### 2. Explicit Over Implicit
- Clear variable names: `precedence_timestamp` not `pt`
- Detailed calculation reasons in output
- Comprehensive logging

### 3. Fail Gracefully
- Never crash on missing data
- Always provide fallback calculations
- Log warnings for data quality issues

### 4. Memoization for Performance
```python
# Pattern used in stage_calculator.py:
if stage_id in self.calculated_adjustments:
    return self.calculated_adjustments[stage_id]
# ... calculate ...
self.calculated_adjustments[stage_id] = result
return result
```

### 5. Type Safety with Pydantic
- All configuration validated on load
- Clear error messages for config issues
- IDE autocomplete support

---

## 🔍 Troubleshooting Guide

### Common Issues & Solutions

#### 1. "Circular dependency detected"
**Cause**: Stage A depends on B, B depends on A
**Solution**: Review `preceding_stage` in config, break the cycle

#### 2. "Field 'xyz' not found in PO data"
**Cause**: Expression references non-existent column
**Solution**: Check Excel column names, update expression

#### 3. All timestamps are None
**Cause**: Date parsing failed
**Solution**: Check date formats in `convert_date_columns()`

#### 4. Calculation takes forever
**Cause**: Missing memoization or infinite recursion
**Solution**: Check for cycles, ensure memoization working

### Debug Checklist
- [ ] Check `tat_calculation.log` for detailed errors
- [ ] Verify Excel column names match config
- [ ] Validate no circular dependencies
- [ ] Ensure date columns properly converted
- [ ] Check expression syntax is valid

---

## 🔌 Extension Points

### 1. Adding New Expression Functions
Location: `expression_evaluator.py` in `_eval_node()` method
```python
elif func_name == 'your_function':
    # Your implementation
    return result
```

### 2. Custom Output Formats
Location: `tat_processor.py` in `calculate_tat()` method
- Modify result dictionary structure
- Add new analytics/metrics

### 3. Alternative Data Sources
Location: `run_tat_calculation.py` in `TATRunner` class
- Replace Excel with database queries
- Add API integration

### 4. Business Rule Engine
- Extend expression engine with more complex rules
- Add validation logic
- Implement alerts/notifications

### 5. Performance Optimizations
- Parallel processing for large datasets
- Database storage for results
- Caching across runs

---

## 📚 Key Takeaways for New Developers

1. **Start with the config**: Most changes only need `stages_config.json` edits
2. **Follow the data**: Trace through one stage calculation to understand flow
3. **Trust the hierarchy**: Precedence → Actual → Fallback is the core pattern
4. **Expressions are powerful**: Complex logic without code changes
5. **Logging is your friend**: Extensive logging already in place
6. **Test incrementally**: Use sample_size parameter to test small batches
7. **Modular structure**: Each module has a specific responsibility

---

## 🎯 Quick Start Checklist

- [ ] Read this document fully
- [ ] Run the system on sample data
- [ ] Trace through Stage 8 calculation in debugger
- [ ] Modify one stage in config and see results
- [ ] Review the generated JSON output structure
- [ ] Check Excel export format
- [ ] Understand the modular structure
- [ ] Review each module's responsibilities

---

*This document is the living brain of the TAT Calculator project. Update it as the system evolves.*