import sys
import os
import json
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import CompositeFilter
from sql_generator import SQLGenerator
from schema_manager import SchemaManager
from models import DatasetSchema, ColumnSchema

def setup_schema():
    """Set up schema for testing."""
    schema_manager = SchemaManager()
    
    # Add app_opened event (to match the test JSON)
    schema_manager.add_event_metadata(
        "app_opened",
        "events",
        {
            "timestamp": "timestamp",
            "app_version": "string",
            "device_type": "string"
        }
    )
    
    return schema_manager

def generate_sql_from_json(json_str: str) -> str:
    """Generate SQL from JSON string."""
    # Parse JSON to dict
    filter_dict = json.loads(json_str)
    
    # Convert dict to CompositeFilter object
    filter_obj = CompositeFilter(**filter_dict)
    
    # Setup schema and generator
    schema_manager = setup_schema()
    generator = SQLGenerator(schema_manager)
    
    # Generate SQL
    return generator.generate_sql(filter_obj)

def main():
    # Get current date and date 7 days ago
    now = datetime.now()
    seven_days_ago = (now - timedelta(days=7)).strftime('%Y-%m-%d')

    # Test Case: HasDone app_opened exactly 1 time in last 7 days
    has_done_exactly_json = f'''{{
        "operation": "AND",
        "filters": [
            {{
                "type": "event",
                "name": "app_opened",
                "filters": [
                    {{
                        "name": "timestamp",
                        "operator": ">=",
                        "value": "{seven_days_ago}"
                    }}
                ],
                "execution": {{
                    "count": 1,
                    "type": "exactly"
                }}
            }}
        ]
    }}'''

    print("Test Case: HasDone app_opened exactly 1 time in last 7 days")
    print("Generated SQL:")
    print(generate_sql_from_json(has_done_exactly_json))

if __name__ == "__main__":
    main() 