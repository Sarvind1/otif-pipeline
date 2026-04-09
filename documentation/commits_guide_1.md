# Commits Guide - TAT Calculator Project

## Overview
This document tracks significant changes and provides guidance for commit messages in the TAT Calculator project.

## Commit Message Format
```
[Type] Brief description of change

- Detailed bullet point 1
- Detailed bullet point 2
- Impact or reason for change
```

## Change Types
- **Fix**: Bug fixes and corrections
- **Feature**: New functionality
- **Refactor**: Code restructuring without behavior change
- **Config**: Changes to stages_config.json
- **Docs**: Documentation updates
- **Performance**: Performance improvements
- **Tool**: Development tools and utilities

## Recent Changes Log

### 2025-07-01: Add stage configuration validator tool
**Type**: Tool  
**Files Added**: `stage_config_validator.py`  
**Commit SHA**: a26a71e27927f7cafd1318fa0fc1286f549d194c

**Issue**: Users were experiencing various configuration errors when modifying `stages_config.json`, leading to runtime failures with cryptic error messages. There was no way to validate configuration before running the TAT calculator.

**Solution**: 
- **Independent Validation Tool**: Created standalone validator that checks configuration without running calculations
- **Comprehensive Error Detection**: Identifies JSON syntax errors, missing fields, invalid expressions, and unsupported features
- **Detailed Reporting**: Provides categorized errors and warnings with specific context and suggestions
- **Syntax Validation**: Detects common issues like unterminated strings, extra quotes, and malformed expressions
- **Expression Analysis**: Validates expressions against supported functions and identifies undefined variables
- **Dependency Validation**: Checks stage references and detects circular dependencies

**Features**:
- **JSON Syntax Checking**: Detects unterminated strings, extra quotes, malformed structures
- **Field Validation**: Ensures all required fields are present and correctly typed
- **Expression Validation**: Validates expressions against current expression evaluator capabilities
- **Unsupported Feature Detection**: Identifies `cond()` functions and other unsupported syntax
- **Dependency Analysis**: Validates stage references and prevents circular dependencies
- **Detailed Error Reports**: Categorized errors with context and fix suggestions

**Usage**:
```bash
# Validate default config
python stage_config_validator.py

# Validate specific config file
python stage_config_validator.py my_stages_config.json

# Exit codes: 0 = valid, 1 = invalid
```

**Error Categories Detected**:
- **JSON**: Syntax errors, unterminated strings, extra quotes
- **MISSING_FIELD**: Required fields not present
- **VALIDATION**: Type mismatches, invalid values
- **UNSUPPORTED**: Features not supported by current system
- **SYNTAX**: Expression parsing issues
- **DEPENDENCY**: Invalid stage references

**Impact**: 
- **Prevents Runtime Errors**: Catch configuration issues before running calculations
- **Faster Debugging**: Clear error messages with specific locations and fixes
- **Better User Experience**: Validation feedback instead of cryptic runtime failures
- **Development Efficiency**: Rapid iteration on configuration changes
- **Quality Assurance**: Ensures configuration meets system requirements

### 2025-06-26: Integrate delay information directly into TAT results
**Type**: Feature  
**Files Modified**: `tat_processor.py`, `tat_calculator_main.py`, `run_tat_calculation.py`  
**Commit SHA**: 401d5d4442bd861519f0335bbe64752c15ce1466

**Issue**: Users wanted delay information (`delay_days` and `delay_status`) included directly in TAT results JSON structure, not just in separate delay analysis files. This would provide a comprehensive view of each stage's performance in a single result.

**Solution**: 
- **Enhanced TAT Processor**: Modified `calculate_tat()` to include delay calculations by default
- **Integrated Delay Fields**: Added `delay_days`, `delay_status`, and `delay_reason` to each stage result
- **Delay Summary**: Added comprehensive delay summary to result statistics
- **Excel Export Enhancement**: Updated Excel export to include delay columns for each stage
- **Backward Compatibility**: Maintained `include_delays` parameter for optional delay integration

**Impact**: 
- **Single Source of Truth**: All TAT and delay information in one JSON result
- **Enhanced Analytics**: Rich delay summary statistics in TAT results
- **Better Excel Reports**: Delay columns automatically included in Excel exports
- **Improved User Experience**: No need to cross-reference separate delay files
- **Comprehensive Insights**: Stage-level delay information readily available

### 2025-06-26: Implement organized folder structure for clean file outputs
**Type**: Feature  
**Files Modified**: `run_tat_calculation.py`, `tat_processor.py`, `delay_calculator.py`  
**Commit SHA**: cdfd4e5845b4c6f5abb82d89ebd62f13971f5fc0

**Issue**: All output files (TAT results, delay reports, Excel exports, CSVs, logs) were being saved to the root directory, creating a cluttered and disorganized file structure.

**Solution**: 
- Implemented organized folder structure with segregated directories
- Added automatic folder creation utilities
- Updated all save/export methods to use organized paths
- Enhanced visual output display showing folder structure
- Maintained backward compatibility for existing code

**Impact**: 
- Clean, organized project structure
- Easy file navigation and management
- Better separation of different output types
- Professional project organization
- Scalable structure for future additions

### 2025-06-26: Fix delay calculation to use target_date from stage_calculations
**Type**: Fix  
**Files Modified**: `delay_calculator.py`  
**Commit SHA**: 8750a230e2fb6ebe2b38a0f8b78c9eb8b08f1dcf

**Issue**: Delay calculations were incorrectly using `stage_result["timestamp"]` as the target timestamp, which represents the final calculated completion time rather than the target date for delay analysis.

**Solution**: 
- Modified `calculate_stage_delay()` to extract `target_date` from stage calculation details
- Changed from using `stage_result["timestamp"]` to `stage_result["calculation"]["target_date"]`
- Added robust target timestamp extraction logic with fallback support
- Maintained backward compatibility for different calculation result structures
- Enhanced error handling and logging for missing target_date fields

**Impact**: 
- Delay calculations now use the correct baseline (target_date) for comparison
- More accurate delay analysis and reporting
- Better alignment between TAT calculations and delay analysis
- Improved transparency in delay calculation methodology

## Development Guidelines

### Before Making Changes
1. Review the project_brain.md for architecture understanding
2. Check existing code patterns and naming conventions
3. Ensure changes align with modular structure principles
4. Consider impact on file organization and outputs
5. **Consider delay integration implications for new features**
6. **Validate configuration changes using stage_config_validator.py**

### Change Process
1. Make targeted, specific changes
2. **Validate configuration with stage_config_validator.py**
3. Test changes on sample data
4. Update relevant documentation
5. Ensure organized folder structure is maintained
6. **Verify delay information is properly integrated where applicable**
7. Commit with descriptive message following the format above
8. Update this commits_guide.md with change details

### Configuration Changes
- Most business logic changes should be in `stages_config.json`
- **Always validate configuration with stage_config_validator.py before committing**
- Code changes should only be for new features or bug fixes
- Always validate configuration after changes

### Testing Approach
- **Run stage_config_validator.py first**
- Test with `ts_small.xlsx` first
- Verify calculations match expected results
- Check JSON output structure remains consistent
- **Verify delay information is correctly integrated in TAT results**
- Ensure Excel export functionality works with delay columns
- Verify organized folder structure is created and used correctly

### Validation Tools Usage
#### Stage Configuration Validator
```bash
# Before making configuration changes
python stage_config_validator.py stages_config.json

# Fix all errors before proceeding
# Re-validate after fixes
```

### Output Organization Best Practices
- Always use organized folder structure for new outputs
- Maintain consistent naming conventions with timestamps
- Ensure folder creation before file operations
- Provide clear feedback on where files are saved
- Keep file types properly segregated
- **Include delay information in relevant outputs**

## Future Improvements Tracking

### Planned Enhancements
- [ ] Enhanced delay reporting with trend analysis
- [ ] Performance optimization for large datasets
- [ ] Additional custom functions in expression evaluator
- [ ] Automated testing framework
- [ ] Archive functionality for old outputs
- [ ] Configuration-driven output folder customization
- [ ] Real-time delay alerts and notifications
- [ ] Interactive delay dashboard
- [ ] **Configuration auto-fix suggestions in validator**
- [ ] **Schema validation with JSON Schema**

### Known Issues
- cond() expressions not supported in current expression evaluator
- Stage references (stage_X) not supported in expressions
- Production lead time (plt) variable needs to be defined

## Validation and Quality Assurance

### Configuration Validation Process
1. **Always run validator first**: `python stage_config_validator.py`
2. **Fix all errors** before proceeding with development
3. **Address warnings** for better configuration quality
4. **Re-validate** after making fixes

### Common Configuration Issues
- **JSON Syntax**: Unterminated strings, extra quotes, malformed structures
- **Missing Fields**: Required fields like `handoff_points` in `process_flow`
- **Invalid Expressions**: Unsupported functions, undefined variables
- **Type Errors**: Wrong data types for fields
- **Dependency Issues**: Invalid stage references, circular dependencies

### Error Resolution Guide
- **Unterminated String**: Check for missing quotes or extra quotes
- **Missing handoff_points**: Add empty array `[]` or appropriate team list
- **cond() Not Supported**: Replace with simple arrays or implement cond() function
- **plt Undefined**: Define production lead time field or remove from expressions
- **stage_X References**: Remove stage references or implement stage lookup

---

**Note**: This guide should be updated after every significant change to maintain project history and development patterns.

## Tool Documentation

### Stage Configuration Validator

The `stage_config_validator.py` tool provides comprehensive validation of `stages_config.json` files before running TAT calculations.

#### Features
- **Syntax Validation**: JSON parsing and structure validation
- **Field Validation**: Required fields and data types
- **Expression Analysis**: Supported functions and variables
- **Dependency Checking**: Stage references and circular dependency detection
- **Detailed Reporting**: Categorized errors and warnings with context

#### Usage Examples
```bash
# Basic validation
python stage_config_validator.py

# Validate specific file
python stage_config_validator.py custom_config.json

# Use in scripts (check exit code)
if python stage_config_validator.py; then
    echo "Configuration is valid"
    python run_tat_calculation.py
else
    echo "Fix configuration errors first"
fi
```

#### Output Format
```
🔍 Validating configuration: stages_config.json
============================================================
✅ JSON syntax is valid
✅ Found 31 stages to validate

============================================================
📊 VALIDATION REPORT
============================================================

❌ ERRORS (5):
----------------------------------------
1. [MISSING_FIELD] Missing required field 'handoff_points' in process_flow
   Context: Stage 1

2. [UNSUPPORTED] cond() expressions are not supported by current expression evaluator
   Context: Stage 3 - expression: cond(pi_applicable==1, ['2'], [])

⚠️  WARNINGS (3):
----------------------------------------
1. [VALIDATION] preceding_stage should be array, not string representation
   Context: Stage 2 - use ["1"] instead of "['1']"

📋 SUMMARY:
Status: INVALID
Errors: 5
Warnings: 3

🚨 Fix all errors before running TAT calculator!
```

This tool helps ensure configuration quality and prevents runtime errors by catching issues early in the development process.
