import sys
import os
import json
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
    
    # Add test event dataset
    app_opened_dataset = DatasetSchema(
        id="1",
        name="app_opened",
        sql_name="events_app_opened",
        type="event"
    )
    schema_manager.add_dataset_schema(app_opened_dataset)

    # Add test user dataset
    user_dataset = DatasetSchema(
        id="user",
        name="user",
        sql_name="users",
        type="user"
    )
    schema_manager.add_dataset_schema(user_dataset)

    # Add test columns for event
    schema_manager.add_column_schema(ColumnSchema(
        id="1",
        name="location",
        sql_name="event_location",
        dataset_id="1",
        is_virtual=False
    ))

    # Add test columns for user
    gender_col = ColumnSchema(
        id="2",
        name="gender",
        sql_name="user_gender",
        dataset_id="user",
        is_virtual=False
    )
    location_col = ColumnSchema(
        id="3",
        name="location",
        sql_name="user_location",
        dataset_id="user",
        is_virtual=False
    )
    schema_manager.add_column_schema(gender_col)
    schema_manager.add_column_schema(location_col)

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
    # Test Case: Complex filter with AND/OR operations
    complex_json = '''{
        "operation": "OR",
        "filters": [
            {
                "operation": "AND",
                "filters": [
                    {
                        "type": "event",
                        "name": "app_opened",
                        "filters": [
                            {
                                "name": "location",
                                "operator": "is",
                                "value": "Bangalore"
                            }
                        ]
                    },
                    {
                        "type": "user",
                        "filters": [
                            {
                                "name": "gender",
                                "operator": "is",
                                "value": "male"
                            }
                        ]
                    }
                ]
            },
            {
                "type": "user",
                "filters": [
                    {
                        "name": "location",
                        "operator": "in",
                        "value": ["Delhi", "Bangalore"]
                    }
                ]
            }
        ]
    }'''

    print("Test Case: Complex filter with AND/OR operations")
    print("Generated SQL:")
    print(generate_sql_from_json(complex_json))

if __name__ == "__main__":
    main() 