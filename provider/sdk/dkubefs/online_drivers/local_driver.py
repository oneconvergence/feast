from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
from feast import Entity, FeatureTable, FeatureView, RepoConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.infra.key_encoding_utils import serialize_entity_key
from mysql.connector import connect
import pytz

from provider.sdk.dkubefs.utils import get_dkube_db_config


class LocalDBDriver:
    """ LocalDBDriver class handles batch read of data from
        MySQL DB. This is the only operation that gets handled
        by this class.
    """
    online_store_config = None
    connect_args = None

    def __init__(self, repo_config: RepoConfig) -> None:
        self.online_store_config = repo_config.online_store
        self.dkube_store = get_dkube_db_config() 
        self.connect_args = {
            "autocommit": True,
        }
        host = self.dkube_store["host"]
        port = self.dkube_store["port"]
        user = self.dkube_store["user"]
        password = self.dkube_store["secret"]
        database = self.dkube_store["db"]
        any_val_unset = None in [host, port, user, password] \
            or "" in [host, port, user, password]
        if any_val_unset:
            sys.exit("Config missing for feast store. Please contact administrator.")
        self.connect_args.update(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            auth_plugin='mysql_native_password'
        )

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[
                EntityKeyProto,
                Dict[str, ValueProto],
                datetime,
                Optional[datetime],
            ]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        project = config.project
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(entity_key).hex()
            timestamp = _to_naive_utc(timestamp)
            if created_ts:
                created_ts = _to_naive_utc(created_ts)

            for feature_name, val in values.items():
                self.insert_into_table(
                    project,
                    table,
                    entity_key_bin,
                    feature_name,
                    timestamp,
                    created_ts,
                    val,
                )
            if progress:
                progress(1)

    def insert_into_table(
        self,
        project,
        table,
        entity_key_bin,
        feature_name,
        timestamp,
        created_ts,
        val,
    ):
        with connect(**self.connect_args) as conn:
            with conn.cursor(buffered=True) as cursor:
                _update_query = f"""
                    update {_table_name(project, table)} set value = %s,
                    event_ts = %s, created_ts = %s
                    where (entity_key = %s and feature_name = %s)
                """
                cursor.execute(
                    _update_query,
                    (
                        val.SerializeToString(),
                        timestamp,
                        created_ts,
                        entity_key_bin,
                        feature_name,
                    ),
                )
        with connect(**self.connect_args) as conn:
            with conn.cursor(buffered=True) as cursor:
                _insert_query = f"""
                    insert ignore into {_table_name(project, table)} (entity_key,
                    feature_name, value, event_ts, created_ts) values (
                    %s, %s, %s, %s, %s)
                """
                cursor.execute(
                    _insert_query,
                    (
                        entity_key_bin,
                        feature_name,
                        val.SerializeToString(),
                        timestamp,
                        created_ts,
                    ),
                )

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        project = config.project
        result = list()
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(entity_key).hex()
            _query = f"""
                select entity_key, feature_name, value, event_ts from
                {_table_name(project, table)} where
                entity_key = %s
            """
            with connect(**self.connect_args) as conn:
                with conn.cursor(buffered=True) as cursor:
                    cursor.execute(_query, (entity_key_bin,))
                    res, res_ts = dict(), None
                    for _, _feature_name, _value, _ts in cursor.fetchall():
                        val = ValueProto()
                        val.ParseFromString(_value)
                        res[_feature_name] = val
                        res_ts = _ts

                    if not res:
                        result.append((None, None))
                    else:
                        result.append((res_ts, res))
        return result

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        project = config.project
        with connect(**self.connect_args) as conn:
            with conn.cursor(buffered=True) as cursor:
                for table in tables_to_keep:
                    _create_query = f"""
                        create table IF NOT EXISTS {_table_name(project, table)
                        } (entity_key VARCHAR(512), feature_name VARCHAR(256),
                        value BLOB, event_ts timestamp, created_ts timestamp,
                        PRIMARY KEY(entity_key, feature_name))
                    """
                    cursor.execute(_create_query)
                    _index_query = f"""
                        alter table {_table_name(project, table)} ADD INDEX
                        {_table_name(project, table)}_ek (entity_key)
                    """
                    # cursor.execute(_index_query)

        with connect(**self.connect_args) as conn:
            with conn.cursor(buffered=True) as cursor:
                for table in tables_to_delete:
                    _drop_index = f"""
                        drop index if exists {_table_name(project, table)}_ek on {_table_name(project, table)}
                    """
                    cursor.execute(_drop_index)
                    _drop_table = f"""
                        drop table if exists {_table_name(project, table)}
                    """
                    cursor.execute(_drop_table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        # teardown_infra should remove all deployed infrastructure
        # Replace the code below in order to define your own custom teardown
        # operations
        project = config.project
        with connect(**self.connect_args) as conn:
            with conn.cursor(buffered=True) as cursor:
                for table in tables:
                    _drop_table = f"""
                        drop table if exists {_table_name(project, table)}
                    """
                    cursor.execute(_drop_table)


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)


def _table_name(project, table):
    return "%s_%s" % (project, table.name)
