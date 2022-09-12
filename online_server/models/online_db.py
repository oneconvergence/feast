from datetime import datetime
from typing import List, Optional, Sequence

from pydantic import BaseModel


class Materialize(BaseModel):
    project: str
    start_date: datetime
    end_date: datetime
    feature_views: Optional[List[str]]
    user: Optional[str]
    offline_dataset: Optional[str]


class MaterializeIncremental(BaseModel):
    project: str
    end_date: datetime
    feature_views: Optional[List[str]]
    user: Optional[str]
    offline_dataset: Optional[str]


class InfraUpdate(BaseModel):
    project: str
    tables_to_delete: Sequence[str]
    tables_to_keep: Sequence[str]
    entities_to_keep: Optional[Sequence[str]] = []
    entities_to_delete: Optional[Sequence[str]] = []
    user: Optional[str]
    offline_dataset: Optional[str]


class InfraDelete(BaseModel):
    project: str
    tables: Sequence[str]
    entities: Optional[Sequence[str]] = None
    user: Optional[str]
    offline_dataset: Optional[str]
