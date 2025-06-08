import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_generator import SQLGenerator
from schema_manager import SchemaManager

def setup_schema():
    """Set up schema for testing."""
    schema_manager = SchemaManager()
    
    # Add app_open event
    schema_manager.add_event_metadata(
        "app_open",
        "events",
        {
            "timestamp": "timestamp",
            "app_version": "string",
            "device_type": "string"
        }
    )
    
    return schema_manager

def main():
    # Set up schema
    schema_manager = setup_schema()
    
    # Create SQL generator
    generator = SQLGenerator(schema_manager)
    
    # Create filter for users who opened app at least once in last 7 days
    filter_json = {
        "type": "event",
        "name": "app_open",
        "filters": [
            {
                "name": "timestamp",
                "operator": ">=",
                "value": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            }
        ],
        "execution": {
            "type": "atleast",
            "count": 1
        }
    }
    
    # Generate SQL
    sql = generator.generate_sql(filter_json)
    print("\nGenerated SQL:")
    print(sql)

if __name__ == "__main__":
    main() 