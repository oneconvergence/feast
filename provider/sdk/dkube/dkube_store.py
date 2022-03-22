from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pytz
from feast import RepoConfig
from feast.entity import Entity
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from mysql.connector import connect
from provider.sdk.dkube.utils import get_dkube_db_config
from pydantic import StrictStr
from pydantic.typing import Literal


class DkubeOnlineStoreConfig(FeastConfigBaseModel):
    type: Literal[
        "dkube.dkube_store.DkubeOnlineStore"
    ] = "dkube.dkube_store.DkubeOnlineStore"


class DkubeOnlineStore(OnlineStore):
    online_store_config = None
    connect_args = None

    def initialize(self, config):
        if self.online_store_config:
            return
        self.online_store_config = config.online_store
        self.connect_args = get_dkube_db_config()

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
        self.initialize(config)
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
        self.initialize(config)
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
        self.initialize(config)
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
        self.initialize(config)
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
