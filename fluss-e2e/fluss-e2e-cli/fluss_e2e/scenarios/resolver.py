# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Safe expression resolver for YAML scenario templates."""

from __future__ import annotations

import ast
import operator
import re
from typing import Any, Callable


_EXPR_PATTERN = re.compile(r"\$\{([^}]+)\}")

_COMPARISON_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}

_ARITHMETIC_OPS: dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.FloorDiv: operator.floordiv,
}


def resolve_value(expr: Any, context: dict[str, Any]) -> Any:
    """Resolve a value that may contain ${...} references.

    If the entire value is a single ${...} expression, the resolved type is
    preserved (int, dict, etc.).  If the expression is embedded in a larger
    string, the result is always a string with the reference replaced inline.
    """
    if not isinstance(expr, str):
        return expr

    match = _EXPR_PATTERN.fullmatch(expr)
    if match:
        return _evaluate_expression(match.group(1).strip(), context)

    def _replace(m: re.Match[str]) -> str:
        return str(_evaluate_expression(m.group(1).strip(), context))

    return _EXPR_PATTERN.sub(_replace, expr)


def evaluate_assertion(
    assert_expr: str,
    context: dict[str, Any],
) -> tuple[bool, Any, Any]:
    """Evaluate a comparison assertion.

    Returns (passed, expected, actual).
    """
    for op_str in ("==", "!=", ">=", "<=", ">", "<"):
        if op_str in assert_expr:
            parts = assert_expr.split(op_str, 1)
            if len(parts) == 2:
                left = resolve_value(parts[0].strip(), context)
                right = resolve_value(parts[1].strip(), context)
                op_func = _COMPARISON_OPS[op_str]
                return op_func(left, right), right, left

    if " in " in assert_expr:
        left_part, right_part = assert_expr.split(" in ", 1)
        left = resolve_value(left_part.strip(), context)
        right = resolve_value(right_part.strip(), context)
        return left in right, right, left

    raise ValueError(f"Unsupported assertion expression: {assert_expr}")


def register_function(
    functions: dict[str, Callable[..., Any]],
    name: str,
    func: Callable[..., Any],
) -> None:
    """Register a built-in function available inside ${...} expressions."""
    functions[name] = func


def _evaluate_expression(expr: str, context: dict[str, Any]) -> Any:
    """Evaluate a single expression from inside ${...}."""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError:
        return _resolve_dotted_path(expr, context)

    return _eval_node(tree.body, context)


def _eval_node(node: ast.AST, context: dict[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        if node.id in context:
            return context[node.id]
        raise KeyError(f"Unknown variable `{node.id}` in expression context")

    if isinstance(node, ast.Attribute):
        obj = _eval_node(node.value, context)
        if isinstance(obj, dict):
            return obj[node.attr]
        return getattr(obj, node.attr)

    if isinstance(node, ast.Subscript):
        obj = _eval_node(node.value, context)
        key = _eval_node(node.slice, context)
        return obj[key]

    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, context)
        right = _eval_node(node.right, context)
        op_func = _ARITHMETIC_OPS.get(type(node.op))
        if op_func is None:
            raise ValueError(f"Unsupported binary operator: {type(node.op).__name__}")
        return op_func(left, right)

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_eval_node(node.operand, context)

    if isinstance(node, ast.Call):
        func = _eval_node(node.func, context)
        args = [_eval_node(arg, context) for arg in node.args]
        return func(*args)

    if isinstance(node, ast.List):
        return [_eval_node(elt, context) for elt in node.elts]

    if isinstance(node, ast.Tuple):
        return tuple(_eval_node(elt, context) for elt in node.elts)

    if isinstance(node, ast.Set):
        return {_eval_node(elt, context) for elt in node.elts}

    raise ValueError(f"Unsupported AST node: {type(node).__name__}")


def _resolve_dotted_path(path: str, context: dict[str, Any]) -> Any:
    parts = path.split(".")
    current: Any = context
    for part in parts:
        if isinstance(current, dict):
            current = current[part]
        else:
            current = getattr(current, part)
    return current
