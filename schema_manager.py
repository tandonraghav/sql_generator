from typing import Dict, Optional, List, Any
from models import DatasetSchema, ColumnSchema

class SchemaManager:
    """
    Manages dataset and column schemas, providing methods to resolve event metadata and attributes.
    """
    def __init__(self, mongo_manager=None, tenant_id: str = None):
        self.datasets: Dict[str, DatasetSchema] = {}
        self.columns: Dict[str, ColumnSchema] = {}
        self.mongo_manager = mongo_manager
        self.tenant_id = tenant_id
        self.event_metadata = {}
        self.user_attributes = {}

    def add_dataset_schema(self, dataset: DatasetSchema) -> None:
        """Add dataset schema to the manager."""
        self.datasets[dataset.name] = dataset

    def add_column_schema(self, column: ColumnSchema) -> None:
        """Add column schema to the manager."""
        self.columns[column.name] = column

    def get_event_attributes(self, event_name: str) -> Dict[str, str]:
        """
        Given an event name, returns a mapping of attribute names to their database column names.
        Example: {'location': 'event_location', 'timestamp': 'event_timestamp'}
        """
        dataset = self.get_dataset(event_name)
        if not dataset:
            return {}
        
        attributes = {}
        for col in self.columns.values():
            if col.dataset_id == dataset.id:
                attributes[col.name] = col.sql_name
        return attributes

    def get_event_metadata(self, event_name: str) -> Dict[str, Any]:
        """
        Given an event name, returns the event's metadata including table name and attributes.
        Example: {
            'table_name': 'events_app_opened',
            'attributes': {
                'location': 'event_location',
                'timestamp': 'event_timestamp'
            }
        }
        """
        return self.event_metadata.get(event_name)

    def get_dataset(self, name: str) -> Optional[DatasetSchema]:
        """Get dataset schema by name."""
        return self.datasets.get(name)

    def get_column(self, name: str) -> Optional[ColumnSchema]:
        """Get column schema by name."""
        return self.columns.get(name)

    def add_event_metadata(self, event_name: str, table_name: str, attributes: Dict[str, str]):
        """Add event metadata to the schema."""
        self.event_metadata[event_name] = {
            "table_name": table_name,
            "attributes": attributes
        }

    def add_user_attribute(self, attribute_name: str, column_name: str):
        """Add user attribute mapping."""
        self.user_attributes[attribute_name] = column_name

    def get_user_attribute(self, attribute_name: str) -> str:
        """Get column name for a user attribute."""
        return self.user_attributes.get(attribute_name, attribute_name) 