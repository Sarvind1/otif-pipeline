"""
Stage Configuration Validator
============================

Independent validation tool to check stages_config.json against stage_calculator requirements.
Identifies syntax errors, missing fields, invalid expressions, and compatibility issues.
"""

import json
import ast
import re
from typing import Dict, List, Any, Tuple
from pathlib import Path
import pandas as pd


class StageConfigValidator:
    """Validates stage configuration against stage_calculator requirements"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.supported_functions = ['max', 'add_days']
        self.required_fields = {
            'stage': ['name', 'actual_timestamp', 'preceding_stage', 'process_flow', 'fallback_calculation', 'lead_time'],
            'process_flow': ['critical_path', 'parallel_processes', 'process_type', 'team_owner', 'handoff_points'],
            'fallback_calculation': ['expression']
        }
    
    def validate_config_file(self, config_path: str) -> Dict[str, Any]:
        """Main validation method"""
        print(f"🔍 Validating configuration: {config_path}")
        print("=" * 60)
        
        # Load and parse JSON
        config_data = self._load_json(config_path)
        if not config_data:
            return self._generate_report()
        
        # Validate structure
        self._validate_top_level_structure(config_data)
        
        # Validate each stage
        if 'stages' in config_data:
            for stage_id, stage_config in config_data['stages'].items():
                self._validate_stage(stage_id, stage_config)
        
        # Cross-validate dependencies
        self._validate_dependencies(config_data.get('stages', {}))
        
        return self._generate_report()
    
    def _load_json(self, config_path: str) -> Dict[str, Any]:
        """Load and validate JSON syntax"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for common JSON issues
            self._check_json_syntax_issues(content)
            
            # Parse JSON
            config_data = json.loads(content)
            print("✅ JSON syntax is valid")
            return config_data
            
        except FileNotFoundError:
            self._add_error("FILE", "Configuration file not found", config_path)
        except json.JSONDecodeError as e:
            self._add_error("JSON", f"JSON parsing error: {e}", f"Line {e.lineno}")
        except Exception as e:
            self._add_error("FILE", f"Error reading file: {e}", config_path)
        
        return {}
    
    def _check_json_syntax_issues(self, content: str):
        """Check for common JSON syntax issues"""
        lines = content.split('\n')
        
        for i, line in enumerate(lines, 1):
            # Check for unterminated strings
            if line.count('"') % 2 != 0:
                # More detailed check
                in_string = False
                escape_next = False
                for j, char in enumerate(line):
                    if escape_next:
                        escape_next = False
                        continue
                    if char == '\\':
                        escape_next = True
                        continue
                    if char == '"':
                        in_string = not in_string
                
                if in_string:
                    self._add_error("JSON", "Potential unterminated string", f"Line {i}: {line.strip()}")
            
            # Check for extra quotes
            if "')" in line and '")' not in line:
                self._add_error("JSON", "Extra quote detected", f"Line {i}: {line.strip()}")
    
    def _validate_top_level_structure(self, config_data: Dict[str, Any]):
        """Validate top-level structure"""
        if 'stages' not in config_data:
            self._add_error("STRUCTURE", "Missing 'stages' key", "Root level")
            return
        
        if not isinstance(config_data['stages'], dict):
            self._add_error("STRUCTURE", "'stages' must be a dictionary", "Root level")
            return
        
        if len(config_data['stages']) == 0:
            self._add_warning("STRUCTURE", "No stages defined", "stages")
        
        print(f"✅ Found {len(config_data['stages'])} stages to validate")
    
    def _validate_stage(self, stage_id: str, stage_config: Dict[str, Any]):
        """Validate individual stage configuration"""
        context = f"Stage {stage_id}"
        
        # Check required fields
        for field in self.required_fields['stage']:
            if field not in stage_config:
                if field == 'handoff_points':
                    # This is in process_flow, check there
                    continue
                self._add_error("MISSING_FIELD", f"Missing required field '{field}'", context)
        
        # Validate each field
        self._validate_stage_name(stage_id, stage_config.get('name'), context)
        self._validate_actual_timestamp(stage_config.get('actual_timestamp'), context)
        self._validate_preceding_stage(stage_config.get('preceding_stage'), context)
        self._validate_process_flow(stage_config.get('process_flow'), context)
        self._validate_fallback_calculation(stage_config.get('fallback_calculation'), context)
        self._validate_lead_time(stage_config.get('lead_time'), context)
    
    def _validate_stage_name(self, stage_id: str, name: Any, context: str):
        """Validate stage name"""
        if not name:
            self._add_error("VALIDATION", "Stage name cannot be empty", context)
        elif not isinstance(name, str):
            self._add_error("VALIDATION", "Stage name must be a string", context)
    
    def _validate_actual_timestamp(self, actual_timestamp: Any, context: str):
        """Validate actual_timestamp field"""
        if actual_timestamp is None:
            return  # Optional field
        
        if not isinstance(actual_timestamp, str):
            self._add_error("VALIDATION", "actual_timestamp must be a string or null", context)
            return
        
        # Check if it contains expressions (should be field names only)
        if 'cond(' in actual_timestamp:
            self._add_error("VALIDATION", "actual_timestamp should be field name, not conditional expression", 
                          f"{context} - found: {actual_timestamp}")
        
        if any(char in actual_timestamp for char in ['(', ')', '[', ']', '==']):
            self._add_warning("VALIDATION", "actual_timestamp contains expression syntax - should be field name only", 
                            f"{context} - found: {actual_timestamp}")
    
    def _validate_preceding_stage(self, preceding_stage: Any, context: str):
        """Validate preceding_stage field"""
        if preceding_stage is None:
            return  # Valid for first stage
        
        if isinstance(preceding_stage, str):
            # Check for conditional expressions
            if 'cond(' in preceding_stage:
                self._validate_conditional_expression(preceding_stage, context)
            elif preceding_stage.startswith("['") and preceding_stage.endswith("']"):
                self._add_warning("VALIDATION", "preceding_stage should be array, not string representation", 
                                f"{context} - use [\"1\"] instead of \"['1']\"")
            elif preceding_stage == "":
                self._add_warning("VALIDATION", "Empty preceding_stage should be null", context)
        elif isinstance(preceding_stage, list):
            # Validate array format
            for item in preceding_stage:
                if not isinstance(item, str):
                    self._add_error("VALIDATION", "preceding_stage array items must be strings", context)
        else:
            self._add_error("VALIDATION", "preceding_stage must be string, array, or null", context)
    
    def _validate_conditional_expression(self, expression: str, context: str):
        """Validate conditional expressions (cond functions)"""
        self._add_error("UNSUPPORTED", "cond() expressions are not supported by current expression evaluator", 
                       f"{context} - expression: {expression}")
        
        # Check for syntax issues in conditional
        if expression.count('(') != expression.count(')'):
            self._add_error("SYNTAX", "Unmatched parentheses in conditional expression", context)
        
        # Check for == vs = usage
        if '==' in expression and '=' in expression.replace('==', ''):
            self._add_error("SYNTAX", "Mixed = and == operators (use == for comparison)", context)
    
    def _validate_process_flow(self, process_flow: Dict[str, Any], context: str):
        """Validate process_flow configuration"""
        if not process_flow:
            self._add_error("MISSING_FIELD", "process_flow is required", context)
            return
        
        # Check required fields
        for field in self.required_fields['process_flow']:
            if field not in process_flow:
                if field == 'handoff_points':
                    self._add_error("MISSING_FIELD", f"Missing required field 'handoff_points' in process_flow", context)
                else:
                    self._add_error("MISSING_FIELD", f"Missing required field '{field}' in process_flow", context)
        
        # Validate field types
        if 'critical_path' in process_flow and not isinstance(process_flow['critical_path'], bool):
            self._add_error("VALIDATION", "critical_path must be boolean", context)
        
        if 'parallel_processes' in process_flow:
            if not isinstance(process_flow['parallel_processes'], list):
                self._add_error("VALIDATION", "parallel_processes must be array", context)
        
        if 'handoff_points' in process_flow:
            if not isinstance(process_flow['handoff_points'], list):
                self._add_error("VALIDATION", "handoff_points must be array", context)
    
    def _validate_fallback_calculation(self, fallback_calc: Dict[str, Any], context: str):
        """Validate fallback_calculation configuration"""
        if not fallback_calc:
            self._add_error("MISSING_FIELD", "fallback_calculation is required", context)
            return
        
        if 'expression' not in fallback_calc:
            self._add_error("MISSING_FIELD", "Missing 'expression' in fallback_calculation", context)
            return
        
        expression = fallback_calc['expression']
        if expression and expression != "None":
            self._validate_expression(expression, context)
    
    def _validate_expression(self, expression: str, context: str):
        """Validate expression syntax"""
        if not expression:
            return
        
        # Check for unsupported variables
        unsupported_vars = ['plt', 'stage_']
        for var in unsupported_vars:
            if var in expression:
                if var == 'plt':
                    self._add_error("VALIDATION", "Variable 'plt' not defined in expression evaluator", 
                                  f"{context} - expression: {expression}")
                elif 'stage_' in expression:
                    self._add_error("VALIDATION", "Stage references (stage_X) not supported in expressions", 
                                  f"{context} - expression: {expression}")
        
        # Check for syntax issues
        if ' -' in expression and 'add_days' in expression:
            self._add_warning("SYNTAX", "Space before minus sign may cause parsing issues", 
                            f"{context} - use 'plt-21' instead of 'plt -21'")
        
        # Try to parse expression
        try:
            # Simple AST check
            ast.parse(expression, mode='eval')
        except SyntaxError as e:
            self._add_error("SYNTAX", f"Invalid Python expression syntax: {e}", 
                          f"{context} - expression: {expression}")
        
        # Check for supported functions
        functions_used = re.findall(r'(\w+)\s*\(', expression)
        for func in functions_used:
            if func not in self.supported_functions:
                self._add_warning("VALIDATION", f"Function '{func}' may not be supported", 
                                f"{context} - supported: {self.supported_functions}")
    
    def _validate_lead_time(self, lead_time: Any, context: str):
        """Validate lead_time field"""
        if lead_time is None:
            self._add_error("MISSING_FIELD", "lead_time is required", context)
            return
        
        if not isinstance(lead_time, int):
            self._add_error("VALIDATION", "lead_time must be integer", context)
        elif lead_time < 0:
            self._add_error("VALIDATION", "lead_time cannot be negative", context)
    
    def _validate_dependencies(self, stages: Dict[str, Any]):
        """Validate stage dependencies"""
        stage_ids = set(stages.keys())
        
        for stage_id, stage_config in stages.items():
            preceding = stage_config.get('preceding_stage')
            
            if isinstance(preceding, str) and preceding.startswith("['") and preceding.endswith("']"):
                # Extract stage IDs from string representation
                try:
                    # Simple extraction for ["1"] format
                    deps = [x.strip("'\"") for x in preceding.strip("[]").split(',')]
                    for dep in deps:
                        dep = dep.strip()
                        if dep and dep not in stage_ids:
                            self._add_error("DEPENDENCY", f"References non-existent stage '{dep}'", 
                                          f"Stage {stage_id}")
                except:
                    pass  # Skip complex parsing for now
            
            elif isinstance(preceding, list):
                for dep in preceding:
                    if dep not in stage_ids:
                        self._add_error("DEPENDENCY", f"References non-existent stage '{dep}'", 
                                      f"Stage {stage_id}")
    
    def _add_error(self, category: str, message: str, context: str):
        """Add error to list"""
        self.errors.append({
            'category': category,
            'severity': 'ERROR',
            'message': message,
            'context': context
        })
    
    def _add_warning(self, category: str, message: str, context: str):
        """Add warning to list"""
        self.warnings.append({
            'category': category,
            'severity': 'WARNING',
            'message': message,
            'context': context
        })
    
    def _generate_report(self) -> Dict[str, Any]:
        """Generate validation report"""
        print("\n" + "=" * 60)
        print("📊 VALIDATION REPORT")
        print("=" * 60)
        
        if not self.errors and not self.warnings:
            print("✅ Configuration is valid!")
            return {'status': 'VALID', 'errors': [], 'warnings': []}
        
        # Print errors
        if self.errors:
            print(f"\n❌ ERRORS ({len(self.errors)}):")
            print("-" * 40)
            for i, error in enumerate(self.errors, 1):
                print(f"{i}. [{error['category']}] {error['message']}")
                print(f"   Context: {error['context']}")
                print()
        
        # Print warnings
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            print("-" * 40)
            for i, warning in enumerate(self.warnings, 1):
                print(f"{i}. [{warning['category']}] {warning['message']}")
                print(f"   Context: {warning['context']}")
                print()
        
        # Summary
        status = 'INVALID' if self.errors else 'VALID_WITH_WARNINGS'
        print(f"\n📋 SUMMARY:")
        print(f"Status: {status}")
        print(f"Errors: {len(self.errors)}")
        print(f"Warnings: {len(self.warnings)}")
        
        if self.errors:
            print(f"\n🚨 Fix all errors before running TAT calculator!")
        
        return {
            'status': status,
            'errors': self.errors,
            'warnings': self.warnings,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings)
        }


def main():
    """Main execution function"""
    import sys
    
    # Get config file path
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "stages_config.json"
    
    # Validate configuration
    validator = StageConfigValidator()
    report = validator.validate_config_file(config_path)
    
    # Exit with appropriate code
    if report['status'] == 'INVALID':
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
