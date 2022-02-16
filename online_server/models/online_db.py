from datetime import datetime
from typing import List, Optional, Sequence
from pydantic import BaseModel


class Materialize(BaseModel):
    start_date: datetime
    end_date: datetime
    feature_views: Optional[List[str]]


class MaterializeIncremental(BaseModel):
    end_date: datetime
    feature_views: Optional[List[str]]


class InfraUpdate(BaseModel):
    project: str
    tables_to_delete: Sequence[str]
    tables_to_keep: Sequence[str]
    entities_to_keep: Optional[Sequence[str]] = []
    entities_to_delete: Optional[Sequence[str]] = []


class InfraDelete(BaseModel):
    project: str
    tables: Sequence[str]
    entities: Optional[Sequence[str]] = None
