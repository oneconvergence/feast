# import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pytz
from feast import RepoConfig
from feast.entity import Entity
from feast.feature_view import FeatureView

# from feast.infra.key_encoding_utils import serialize_entity_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from mysql.connector import connect
from provider.sdk.dkubefs.online_drivers.online_server_client import (
    OnlineServerClient,
)
from provider.sdk.dkubefs.utils import (
    get_dkube_server_config,
    get_dkube_server_host,
    get_offline_dataset,
    get_user,
)


class OnlineRemoteDriver:
    """OnlineRemoteDriver proxies online store API calls to Feast Knative
    service from client SDK.
    """

    online_server_client: Optional[OnlineServerClient] = None

    def __init__(self, config: RepoConfig) -> None:
        if not self.online_server_client:
            self.dkube_server = get_dkube_server_config()
            self.online_server_client = OnlineServerClient(
                # dkube_ip=self.dkube_server["host"],
                # dkube_port=self.dkube_server["port"],
                dkube_url=self.dkube_server,
                token="",
                dkube_endpoint=False,
            )
            self.online_server_host = get_dkube_server_host()
            self.user = get_user()
            self.offline_dataset = get_offline_dataset()

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
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
        """This function is a just placeholder. The call gets
        handled in call_materialize().
        """
        # project = config.project
        # for entity_key, values, timestamp, created_ts in data:
        #     entity_key_bin = serialize_entity_key(entity_key).hex()
        #     timestamp = _to_naive_utc(timestamp)
        #     if created_ts:
        #         created_ts = _to_naive_utc(created_ts)

        #     for feature_name, val in values.items():
        #         self.insert_into_table(
        #             project,
        #             table,
        #             entity_key_bin,
        #             feature_name,
        #             timestamp,
        #             created_ts,
        #             val,
        #         )
        #     if progress:
        #         progress(1)
        pass

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
        """Not used in SDK any more."""
        # with connect(**self.connect_args) as conn:
        #     with conn.cursor(buffered=True) as cursor:
        #         _update_query = f"""
        #             update {_table_name(project, table)} set value = %s,
        #             event_ts = %s, created_ts = %s
        #             where (entity_key = %s and feature_name = %s)
        #         """
        #         cursor.execute(
        #             _update_query,
        #             (
        #                 val.SerializeToString(),
        #                 timestamp,
        #                 created_ts,
        #                 entity_key_bin,
        #                 feature_name,
        #             ),
        #         )
        # with connect(**self.connect_args) as conn:
        #     with conn.cursor(buffered=True) as cursor:
        #         _insert_query = f"""
        #             insert ignore into {_table_name(project, table)} (entity_key,
        #             feature_name, value, event_ts, created_ts) values (
        #             %s, %s, %s, %s, %s)
        #         """
        #         cursor.execute(
        #             _insert_query,
        #             (
        #                 entity_key_bin,
        #                 feature_name,
        #                 val.SerializeToString(),
        #                 timestamp,
        #                 created_ts,
        #             ),
        #         )
        pass

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Placeholder only. Not used with remote driver."""
        # project = config.project
        # result = list()
        # for entity_key in entity_keys:
        #     entity_key_bin = serialize_entity_key(entity_key).hex()
        #     _query = f"""
        #         select entity_key, feature_name, value, event_ts from
        #         {_table_name(project, table)} where
        #         entity_key = %s
        #     """
        #     with connect(**self.connect_args) as conn:
        #         with conn.cursor(buffered=True) as cursor:
        #             cursor.execute(_query, (entity_key_bin,))
        #             res, res_ts = dict(), None
        #             for _, _feature_name, _value, _ts in cursor.fetchall():
        #                 val = ValueProto()
        #                 val.ParseFromString(_value)
        #                 res[_feature_name] = val
        #                 res_ts = _ts

        #             if not res:
        #                 result.append((None, None))
        #             else:
        #                 result.append((res_ts, res))
        # return result
        pass

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        project = config.project
        (
            keep_tables_names,
            delete_tables_names,
            keep_entities_names,
            delete_entities_names,
        ) = preprocess_infra_tables(
            project=project,
            tables_to_delete=tables_to_delete,
            tables_to_keep=tables_to_keep,
            entities_to_delete=entities_to_delete,
            entities_to_keep=entities_to_keep,
        )
        tables_data = dict(
            project=project,
            tables_to_delete=delete_tables_names,
            tables_to_keep=keep_tables_names,
            entities_to_keep=keep_entities_names,
            entities_to_delete=delete_entities_names,
            user=self.user,
            offline_dataset=self.offline_dataset,
        )
        self.online_server_client.post(
            "api/v1/infra_update",
            data=tables_data,
            headers=self.online_server_host,
        )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        # teardown_infra should remove all deployed infrastructure
        # Replace the code below in order to define your own custom teardown
        # operations
        project = config.project
        tables_to_teardown = preprocess_teardown_tables(project, tables)
        teardown_data = {
            "project": project,
            "tables": tables_to_teardown,
            "user": self.user,
            "offline_dataset": self.offline_dataset,
        }
        self.online_server_client.delete(
            "api/v1/teardown",
            data=teardown_data,
            headers=self.online_server_host,
        )

    def call_materialize(
        self,
        project: str,
        start_date: datetime,
        end_date: datetime,
        feature_views: Optional[List[str]] = None,
    ) -> None:
        materialize_data = {
            "project": project,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "feature_views": feature_views,
            "user": self.user,
            "offline_dataset": self.offline_dataset,
        }
        self.online_server_client.post(
            "api/v1/materialize",
            data=materialize_data,
            headers=self.online_server_host,
        )

    def call_materialize_incremental(
        self,
        project: str,
        end_date: datetime,
        feature_views: Optional[List[str]] = None,
    ) -> None:
        materialize_data = {
            "project": project,
            "end_date": end_date.isoformat(),
            "feature_views": feature_views,
            "user": self.user,
            "offline_dataset": self.offline_dataset,
        }
        self.online_server_client.post(
            "api/v1/materialize_incr",
            data=materialize_data,
            headers=self.online_server_host,
        )


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)


def _table_name(project, table):
    return "%s_%s" % (project, table.name)


def preprocess_infra_tables(
    project: str,
    tables_to_delete: Sequence[FeatureView],
    tables_to_keep: Sequence[FeatureView],
    entities_to_delete: Sequence[Entity],
    entities_to_keep: Sequence[Entity],
) -> Tuple[Sequence[str], Sequence[str], Sequence[str], Sequence[str]]:
    keep_tables = [_table_name(project, table) for table in tables_to_keep]
    delete_tables = [_table_name(project, table) for table in tables_to_delete]
    entities_delete = [
        _table_name(project, table) for table in entities_to_delete
    ]
    entities_keep = [_table_name(project, table) for table in entities_to_keep]
    return keep_tables, delete_tables, entities_keep, entities_delete


def preprocess_teardown_tables(
    project, tables: Sequence[FeatureView]
) -> Sequence[str]:
    return [_table_name(project, table) for table in tables]
