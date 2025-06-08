from typing import List, Optional
from pymongo import MongoClient
from models import DatasetSchema, ColumnSchema
from datetime import datetime

class MongoManager:
    """
    Manages MongoDB connections and fetches schema details.
    """
    def __init__(self, connection_string: str):
        self.client = MongoClient(connection_string)
        self.db = self.client.schema_db  # Use a fixed database name for schemas

    def get_dataset_schemas(self) -> List[DatasetSchema]:
        """Fetch all dataset schemas for the tenant."""
        datasets = self.db.schemas.find({"category": {"$in": ["event", "user", "notification", "events_aggregated"]}})
        return [DatasetSchema(**self._convert_dates(dataset)) for dataset in datasets]

    def get_dataset_by_name(self, name: str, account_id: str, tenant_id: str) -> Optional[DatasetSchema]:
        """Fetch a specific dataset schema by name, account_id and tenant_id."""
        dataset = self.db.schemas.find_one({
            "name": name,
            "account_id": account_id,
            "tenant_id": tenant_id,
            "category": {"$in": ["event", "user"]}
        })
        return DatasetSchema(**self._convert_dates(dataset)) if dataset else None

    def get_columns_by_dataset_id(self, dataset_id: str, tenant_id: str) -> List[ColumnSchema]:
        """Fetch all columns for a specific dataset using dataset_id and tenant_id."""
        columns = self.db.schemas.find({
            "dataset_id": dataset_id,
            "tenant_id": tenant_id,
            "data_type": {"$exists": True}
        })
        return [ColumnSchema(**self._convert_dates(column)) for column in columns]

    def get_column_schema(self, dataset_id: str, column_name: str, tenant_id: str) -> Optional[ColumnSchema]:
        """Fetch a specific column schema by dataset_id, column name and tenant_id."""
        column = self.db.schemas.find_one({
            "dataset_id": dataset_id,
            "name": column_name,
            "tenant_id": tenant_id
        })
        return ColumnSchema(**self._convert_dates(column)) if column else None

    def _convert_dates(self, doc: dict) -> dict:
        """Convert MongoDB date fields to Python datetime objects."""
        if doc:
            for field in ["created_at", "updated_at"]:
                if field in doc and isinstance(doc[field], str):
                    doc[field] = datetime.fromisoformat(doc[field].replace("Z", "+00:00"))
        return doc 