from typing import List, Dict, Any
from models import CompositeFilter
from operators import SQLOperator
from schema_manager import SchemaManager

class UserGenerator:
    """
    Generates SQL CTEs for user filters.
    """
    def __init__(self, schema_manager: SchemaManager):
        self.sql_operator = SQLOperator()
        self.cte_counter = 0
        self.schema_manager = schema_manager

    def get_user_attributes(self) -> Dict[str, str]:
        """
        Returns a mapping of user attribute names to their database column names.
        Example: {'gender': 'user_gender', 'location': 'user_location'}
        """
        return self.schema_manager.get_event_attributes("user")

    def generate_user_cte(self, filter_dict: Dict[str, Any], cte_name: str) -> str:
        """Generate CTE for user-based filters."""
        # Get user attributes
        user_attributes = self.get_user_attributes()
        
        # Build conditions using actual column names
        conditions = []
        for f in filter_dict["filters"]:
            column_name = user_attributes.get(f['name'], f['name'])
            conditions.append(
                f"{column_name} {self.sql_operator.get_sql_operator(f['operator'])} {self.sql_operator.format_value(f['value'])}"
            )
        
        conditions_str = " AND ".join(conditions)
        return f"{cte_name} AS (\n    SELECT user_id\n    FROM users\n    WHERE {conditions_str}\n    GROUP BY user_id\n)" 