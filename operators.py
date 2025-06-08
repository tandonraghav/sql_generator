class SQLOperator:
    """Handles SQL operator conversions and value formatting."""
    
    @staticmethod
    def get_sql_operator(operator: str) -> str:
        """Convert filter operator to SQL operator."""
        operator_map = {
            "is": "=",
            "in": "IN",
            "not_in": "NOT IN",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<="
        }
        return operator_map.get(operator.lower(), operator)

    @staticmethod
    def format_value(value: any) -> str:
        """Format value for SQL query."""
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, list):
            return f"({', '.join(SQLOperator.format_value(v) for v in value)})"
        return str(value) 