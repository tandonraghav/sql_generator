from typing import List, Dict, Any, Union
from pydantic import BaseModel

class Filter(BaseModel):
    """Base filter model."""
    type: str
    operator: str
    value: Any

class ColumnFilter(Filter):
    """Column-based filter."""
    name: str

class DatasetSchema(BaseModel):
    id: str
    name: str
    sql_name: str
    type: str

class ColumnSchema(BaseModel):
    id: str
    name: str
    sql_name: str
    dataset_id: str
    is_virtual: bool = False
    source: str = None

class CompositeFilter(BaseModel):
    operation: str  # "AND" or "OR"
    filters: List[Union[Dict[str, Any], "CompositeFilter"]] 