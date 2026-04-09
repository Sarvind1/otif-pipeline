"""
Expression Evaluation Engine
============================

Dynamic expression evaluation with custom functions for TAT calculations.
"""

import ast
from datetime import datetime, timedelta
from typing import Any, Optional, Tuple
import pandas as pd


class ExpressionEvaluator:
    """
    Evaluates dynamic expressions with custom functions.
    
    Supported functions:
    - max(date1, date2, ...): Returns latest datetime
    - add_days(date, days): Adds days to a date
    - cond(condition, true_val, false_val): Conditional evaluation
    """
    
    def __init__(self):
        self.calculated_adjustments = {}
    
    def set_calculated_adjustments(self, adjustments):
        """Set the calculated adjustments cache for stage references"""
        self.calculated_adjustments = adjustments
    
    def get_date_value(self, field_name: str, po_row: pd.Series) -> Optional[datetime]:
        if field_name not in po_row.index:
            print(f"[warn] Field '{field_name}' not found in PO data")
            return None
        
        value = po_row[field_name]
        
        if pd.isna(value) or value == "" or value == "NA":
            return None
        
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, str):
            try:
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%d/%m/%Y"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                return pd.to_datetime(value)
            except:
                print(f"[warn] Could not parse date from field '{field_name}': {value}")
                return None
        
        return None
    
    def evaluate_expression(self, expression: str, po_row: pd.Series) -> Tuple[Optional[datetime], str]:
        try:
            tree = ast.parse(expression, mode='eval')
            result = self._eval_node(tree.body, po_row)
            
            if isinstance(result, datetime):
                return result, f"Calculation: {expression} = {result.strftime('%Y-%m-%d %H:%M:%S')}"
            elif result is not None:
                return result, f"Calculation: {expression} = {result}"
            else:
                return None, f"Calculation failed: {expression}"
        except Exception as e:
            print(f"[error] Error evaluating expression '{expression}': {e}")
            return None, f"Calculation error: {expression} ({str(e)})"
    
    def _eval_node(self, node: ast.AST, po_row: pd.Series) -> Any:
        if isinstance(node, ast.Name):
            var_name = node.id
            if var_name.startswith('stage_'):
                stage_id = var_name.replace('stage_', '')
                value = self.calculated_adjustments.get(stage_id, (None, {}))[0]
                print(f"[eval] stage variable '{var_name}' -> {value}")
                return value
            else:
                # Use get_date_value first
                value = self.get_date_value(var_name, po_row)
                if value is not None:
                    print(f"[eval] field variable '{var_name}' -> {value} (parsed as date)")
                    return value
                
                # fallback to raw value
                value = po_row.get(var_name)
                print(f"[eval] field variable '{var_name}' -> {value} (raw fallback)")
                return value
        
        elif isinstance(node, ast.Constant):
            print(f"[eval] constant -> {node.value}")
            return node.value
        
        elif isinstance(node, ast.List):
            elements = [self._eval_node(elt, po_row) for elt in node.elts]
            print(f"[eval] list -> {elements}")
            return elements
        
        elif isinstance(node, ast.UnaryOp):
            operand = self._eval_node(node.operand, po_row)
            if isinstance(node.op, ast.USub):
                print(f"[eval] unary -{operand}")
                return -operand
            elif isinstance(node.op, ast.UAdd):
                print(f"[eval] unary +{operand}")
                return +operand
            else:
                raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
        
        elif isinstance(node, ast.BinOp):
            left = self._eval_node(node.left, po_row)
            right = self._eval_node(node.right, po_row)
            print(f"[eval] binary op {type(node.op).__name__} between {left} and {right}")
            
            if left is None or right is None:
                return None
            
            if isinstance(node.op, ast.Add):
                if isinstance(left, datetime) and isinstance(right, (int, float)):
                    return left + timedelta(days=right)
                elif isinstance(right, datetime) and isinstance(left, (int, float)):
                    return right + timedelta(days=left)
                else:
                    return left + right
            
            elif isinstance(node.op, ast.Sub):
                if isinstance(left, datetime) and isinstance(right, (int, float)):
                    return left - timedelta(days=right)
                elif isinstance(left, datetime) and isinstance(right, datetime):
                    return (left - right).days
                else:
                    return left - right
            
            elif isinstance(node.op, ast.Mult):
                return left * right
            
            elif isinstance(node.op, ast.Div):
                return left / right if right != 0 else None
            
            else:
                print(f"[warn] Unsupported binary operation: {type(node.op)}")
                return None
        
        elif isinstance(node, ast.Compare):
            left = self._eval_node(node.left, po_row)
            right = self._eval_node(node.comparators[0], po_row)
            op = node.ops[0]

            print(f"[eval] comparison {left} {type(op).__name__} {right}")

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
            elif isinstance(op, ast.In):
                return left in right
            elif isinstance(op, ast.NotIn):
                return left not in right
            else:
                raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")

        
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
                
                if func_name in ['iff', 'cond']:
                    if len(node.args) != 3:
                        raise ValueError("cond/iff requires exactly 3 arguments")
                    # Evaluate only the condition first
                    condition = self._eval_node(node.args[0], po_row)
                    print(f"[eval] function call '{func_name}' with condition {condition}")
                    
                    if condition:
                        # Evaluate and return true branch only
                        true_val = self._eval_node(node.args[1], po_row)
                        print(f"[eval] function call '{func_name}' true branch -> {true_val}")
                        return true_val
                    else:
                        # Evaluate and return false branch only
                        false_val = self._eval_node(node.args[2], po_row)
                        print(f"[eval] function call '{func_name}' false branch -> {false_val}")
                        return false_val
                
                else:
                    # For other functions, evaluate all arguments eagerly
                    args = [self._eval_node(arg, po_row) for arg in node.args]
                    print(f"[eval] function call '{func_name}' with args {args}")
                    
                    if func_name == 'max':
                        # Filter out None values
                        valid_args = [arg for arg in args if arg is not None]
                        try:
                            return max(valid_args) if valid_args else None
                        except Exception as e:
                            print(f"[error] max() failed on args {valid_args}: {e}")
                            return None
                    
                    elif func_name == 'add_days':
                        if len(args) >= 2 and isinstance(args[0], datetime) and isinstance(args[1], (int, float)):
                            return args[0] + timedelta(days=int(args[1]))
                        return None
                    
                    else:
                        raise ValueError(f"Unknown function: {func_name}")

        
        else:
            raise ValueError(f"Unsupported AST node type: {type(node).__name__}")
