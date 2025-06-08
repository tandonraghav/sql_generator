from typing import List, Dict, Any
from models import CompositeFilter
from generators.event_generator import EventGenerator
from generators.user_generator import UserGenerator
from schema_manager import SchemaManager

class SQLGenerator:
    """Generates SQL queries from filter objects."""
    
    def __init__(self, schema_manager: SchemaManager):
        self.event_generator = EventGenerator(schema_manager)
        self.user_generator = UserGenerator(schema_manager)
        self.all_ctes = []

    def generate_sql(self, filter_obj):
        # Accept both dict and Pydantic model
        if hasattr(filter_obj, "model_dump"):
            filter_dict = filter_obj.model_dump()
        else:
            filter_dict = filter_obj
        
        self.all_ctes = []
        final_query = self._process_filter(filter_dict)
        if not self.all_ctes:
            return "SELECT 1 WHERE FALSE"  # Return no results if no filters
        ctes_sql = ",\n".join(self.all_ctes)
        return f"WITH {ctes_sql}\n{final_query}"

    def _process_filter(self, filter_obj: Dict[str, Any]) -> str:
        # If this is a composite filter (AND/OR)
        if "operation" in filter_obj:
            subqueries = [self._process_filter(f) for f in filter_obj["filters"]]
            op = filter_obj["operation"].strip().upper()
            if op == "AND":
                return f"SELECT user_id FROM ({' INTERSECT '.join(subqueries)})"
            elif op == "OR":
                return f"SELECT user_id FROM ({' UNION '.join(subqueries)})"
            else:
                raise ValueError(f"Unknown operation: {op}")
        # Otherwise, this is a leaf filter (event/user)
        elif filter_obj["type"] == "event":
            cte_name = f"event_cte_{self.event_generator.cte_counter}"
            self.event_generator.cte_counter += 1
            self.all_ctes.append(self.event_generator.generate_event_cte(filter_obj, cte_name))
            return f"SELECT user_id FROM {cte_name}"
        elif filter_obj["type"] == "user":
            cte_name = f"user_cte_{self.user_generator.cte_counter}"
            self.user_generator.cte_counter += 1
            self.all_ctes.append(self.user_generator.generate_user_cte(filter_obj, cte_name))
            return f"SELECT user_id FROM {cte_name}"
        else:
            raise ValueError("Unknown filter type") 