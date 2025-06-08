from typing import List, Dict, Any
from models import CompositeFilter
from operators import SQLOperator
from schema_manager import SchemaManager

class EventGenerator:
    """
    Generates SQL for event-based filters.
    """
    def __init__(self, schema_manager: SchemaManager):
        self.sql_operator = SQLOperator()
        self.cte_counter = 0
        self.schema_manager = schema_manager

    def _get_execution_clause(self, count: int, exec_type: str) -> str:
        """Return the HAVING clause for execution count conditions."""
        if exec_type == "exactly":
            return f"HAVING COUNT(*) = {count}"
        else:
            # Default to 'atleast'
            return f"HAVING COUNT(*) >= {count}"

    def generate_event_cte(self, filter_dict: Dict[str, Any], cte_name: str) -> str:
        """Generate CTE for event-based filters."""
        # Get event metadata
        event_metadata = self.schema_manager.get_event_metadata(filter_dict['name'])
        if not event_metadata:
            raise ValueError(f"Event {filter_dict['name']} not found in schema")

        # Build conditions using actual column names
        conditions = []
        for f in filter_dict["filters"]:
            column_name = event_metadata['attributes'].get(f['name'], f['name'])
            conditions.append(
                f"{column_name} {self.sql_operator.get_sql_operator(f['operator'])} {self.sql_operator.format_value(f['value'])}"
            )
        
        conditions_str = " AND ".join(conditions)
        
        # Handle execution count conditions
        execution = filter_dict.get("execution", {})
        if execution:
            count = execution.get("count", 1)
            exec_type = execution.get("type", "atleast")
            execution_clause = self._get_execution_clause(count, exec_type)
            return f"{cte_name} AS (\n    SELECT user_id\n    FROM {event_metadata['table_name']}\n    WHERE {conditions_str}\n    GROUP BY user_id\n    {execution_clause}\n)"
        
        # Default case: no execution count condition
        return f"{cte_name} AS (\n    SELECT user_id\n    FROM {event_metadata['table_name']}\n    WHERE {conditions_str}\n    GROUP BY user_id\n)" 