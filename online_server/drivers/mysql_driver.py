from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence

from feast.feature_store import FeatureStore
from mysql.connector import connect

from drivers.mysql_config import MysqlConfig
from common.utils.utils import get_repo_path


def materialize(
    project:str,
    start_date: datetime,
    end_date: datetime,
    feature_views: Optional[List[str]] = None
) -> None:
    fs = FeatureStore(repo_path=get_repo_path())
    fs.project = project
    fs.materialize(start_date, end_date, feature_views)


def materialize_incremental(
    project:str,
    end_date: datetime,
    feature_views: Optional[List[str]] = None
) -> None:
    fs = FeatureStore(repo_path=get_repo_path())
    fs.project = project
    fs.materialize_incremental(end_date, feature_views)


def infra_update(
    project: str,
    tables_to_keep: Sequence[str],
    tables_to_delete: Sequence[str],
    entities_to_keep: Optional[Sequence[str]],
    entities_to_delete: Optional[Sequence[str]],
):
    with connect(**MysqlConfig()._CONFIG) as conn:
        with conn.cursor(buffered=True) as cursor:
            for table in tables_to_keep:
                _create_query = f"""
                    create table IF NOT EXISTS {table} (entity_key VARCHAR(512),
                    feature_name VARCHAR(256),
                    value BLOB, event_ts timestamp, created_ts timestamp,
                    PRIMARY KEY(entity_key, feature_name))
                """
                cursor.execute(_create_query)
                _index_query = f"""
                    alter table {table} ADD INDEX
                    {table}_ek (entity_key)
                """
                # cursor.execute(_index_query)

    with connect(**MysqlConfig()._CONFIG) as conn:
        with conn.cursor(buffered=True) as cursor:
            for table in tables_to_delete:
                _drop_index = f"""
                    drop index if exists {table}_ek on {table}
                """
                cursor.execute(_drop_index)
                _drop_table = f"""
                    drop table if exists {table}
                """
                cursor.execute(_drop_table)


def teardown(tables: Sequence[str], entities: Optional[Sequence[str]] = None) -> None:
    with connect(**MysqlConfig()._CONFIG) as conn:
        with conn.cursor(buffered=True) as cursor:
            for table in tables:
                _drop_table = f"""
                    drop table if exists {table}
                """
                cursor.execute(_drop_table)
