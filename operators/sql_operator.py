from typing import Any

class SQLOperator:
    """
    Handles SQL operator translation and value formatting.
    This class can be extended to support new operators or custom logic.
    """
    @staticmethod
    def get_sql_operator(operator: str) -> str:
        operator_map = {
            "is": "=",
            "in": "IN",
            "not_in": "NOT IN",
            "gt": ">",
            "lt": "<",
            "gte": ">=",
            "lte": "<=",
            "contains": "LIKE",
            "not_contains": "NOT LIKE",
            "inthelast": ">="  # For inthelast, we use >= to check if the field is within the last N units
        }
        return operator_map.get(operator, operator)

    @staticmethod
    def format_value(value: Any) -> str:
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, list):
            return f"({', '.join(SQLOperator.format_value(v) for v in value)})"
        return str(value) 