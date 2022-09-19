from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from feast import RepoConfig
from feast.entity import Entity
from feast.feature_view import FeatureView
# from feast.infra.infra_object import InfraObject, MYSQL_INFRA_OBJECT_CLASS_TYPE
from feast.infra.online_stores.online_store import OnlineStore
# from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
# from feast.protos.feast.core.MySQLOnlineTable_pb2 import MySQLOnlineTable as MySQLOnlineTableProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from provider.sdk.dkubefs.online_drivers.local_driver import LocalDBDriver
from provider.sdk.dkubefs.online_drivers.remote_driver import \
    OnlineRemoteDriver
# from mysql.connector import connect
from pydantic.typing import Literal


class DkubeOnlineStoreConfig(FeastConfigBaseModel):
    type: Literal[
        "dkubefs.dkube_store.DkubeOnlineStore"
    ] = "dkubefs.dkube_store.DkubeOnlineStore"


class DkubeOnlineStore(OnlineStore):
    """ Online store of Dkube Feast provider.

    Args:
        OnlineStore : Base class of online store.
    """
    driver: Union[OnlineRemoteDriver, LocalDBDriver] = None

    def initialize(self, config):
        if self.driver:
            return self.driver
        self.driver = {}
        self.driver["remote"] = OnlineRemoteDriver(config)
        self.driver["local"] = LocalDBDriver(config)

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
        self.initialize(config)
        table.name = _table_id(config.project, table)
        self.driver["remote"].online_write_batch(config, table, data, progress)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        self.initialize(config)
        table.name = _table_id(config.project, table)
        return self.driver["local"].online_read(
            config,
            table,
            entity_keys,
            requested_features
        )

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        self.initialize(config)
        for table in tables_to_delete:
            table.name = _table_id(config.project, table)
        for table in tables_to_keep:
            table.name = _table_id(config.project, table)
        self.driver["remote"].update(
            config,
            tables_to_delete,
            tables_to_keep,
            entities_to_delete,
            entities_to_keep, partial
        )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        # teardown_infra should remove all deployed infrastructure
        # Replace the code below in order to define your own custom teardown
        # operations
        self.initialize(config)
        for table in tables:
            table.name = _table_id(config.project, table)
        self.driver["remote"].teardown(config, tables, entities)

    def process_materialize(
        self,
        config: RepoConfig,
        start_date: datetime,
        end_date: datetime,
        feature_views: Optional[List[str]] = None
    ) -> None:
        self.initialize(config)
        self.driver["remote"].call_materialize(
            config.project, start_date, end_date, feature_views
        )

    def process_materialize_incremental(
        self,
        config: RepoConfig,
        end_date: datetime,
        feature_views: Optional[List[str]] = None
    ) -> None:
        self.initialize(config)
        self.driver["remote"].call_materialize_incremental(
            config.project, end_date, feature_views
        )


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


# class MySQLOnlineTable(InfraObject):
#     """ MySQL table managed by Feast

#     Args:
#         InfraObject (_type_): _description_
#     """

#     def __init__(self, name) -> None:
#         super().__init__(name)

#     def to_infra_object_proto(self) -> InfraObjectProto:
#         mysql_online_table_proto = self.to_proto()
#         return InfraObjectProto(
#             infra_object_class_type=MYSQL_INFRA_OBJECT_CLASS_TYPE,
#             mysql_online_table=mysql_online_table_proto,
#         )

#     def to_proto(self) -> Any:
#         mysql_online_table_proto = MySQLOnlineTableProto()
#         mysql_online_table_proto.name = self.name
#         return mysql_online_table_proto

#     @staticmethod
#     def from_infra_object_proto(infra_object_proto: InfraObjectProto) -> Any:
#         return MySQLOnlineTable(
#             name=infra_object_proto.mysql_online_table.name
#         )

#     @staticmethod
#     def from_proto(mysql_online_table_proto: MySQLOnlineTableProto) -> Any:
#         return MySQLOnlineTable(mysql_online_table_proto.name)

#     def update(self):
#         self.conn.execute(
#             f"CREATE TABLE IF NOT EXISTS {self.name} (entity_key BLOB, feature_name TEXT, value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
#         )
#         self.conn.execute(
#             f"CREATE INDEX IF NOT EXISTS {self.name}_ek ON {self.name} (entity_key);"
#         )

#     def teardown(self):
#         self.conn.execute(f"DROP TABLE IF EXISTS {self.name}")
