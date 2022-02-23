from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from feast import RepoConfig
from feast.entity import Entity
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
# from mysql.connector import connect
from pydantic import StrictStr
from pydantic.typing import Literal
from provider.sdk.custom_provider.online_drivers.local_driver import OnlineLocalDriver

from provider.sdk.custom_provider.online_drivers.remote_driver import OnlineRemoteDriver


class CustomOnlineStoreConfig(FeastConfigBaseModel):
    type: Literal[
        "custom_provider.custom_online_store.CustomOnlineStore"
    ] = "custom_provider.custom_online_store.CustomOnlineStore"
    host: Optional[StrictStr] = None
    port: Optional[StrictStr] = None
    user: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    db: Optional[StrictStr] = None


class CustomOnlineStore(OnlineStore):
    driver: Union[OnlineRemoteDriver, OnlineLocalDriver] = None

    def initialize(self, config):
        if self.driver:
            return self.driver
        self.driver = {}
        self.driver["remote"] = OnlineRemoteDriver(config)
        self.driver["local"] = OnlineLocalDriver(config)

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
        self.driver["remote"].online_write_batch(config, table, data, progress)

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        self.initialize(config)
        return self.driver["local"].online_read(config, table, entity_keys, requested_features)

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
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        # teardown_infra should remove all deployed infrastructure
        # Replace the code below in order to define your own custom teardown
        # operations
        self.initialize(config)
        self.driver["remote"].teardown(config, tables, entities)

    def process_materialize(
        self,
        config: RepoConfig,
        start_date: datetime,
        end_date: datetime,
        feature_views:Optional[List[str]] = None
    ) -> None:
        self.initialize(config)
        self.driver["remote"].call_materialize(
            start_date, end_date, feature_views
        )

    def process_materialize_incremental(
        self,
        config: RepoConfig,
        end_date: datetime,
        feature_views:Optional[List[str]] = None
    ) -> None:
        self.initialize(config)
        self.driver["remote"].call_materialize_incremental(
            end_date, feature_views
        )
