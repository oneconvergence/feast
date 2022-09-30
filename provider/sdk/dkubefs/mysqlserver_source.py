import json
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

import pandas as pd
from feast import RepoConfig, ValueType
from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import (
    DataSource as DataSourceProto,
)
from mysql.connector import connect
from provider.sdk.dkubefs.utils import (
    get_mysql_connect_args,
    get_offline_connection_str,
)


class MySQLOptions:
    def __init__(self, connection_str, table_ref):
        self._connection_str = connection_str
        self._table_ref = table_ref

    @property
    def connection_str(self):
        return self._connection_str

    @connection_str.setter
    def connection_str(self, connection_str):
        self._connection_str = connection_str

    @property
    def table_ref(self):
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_ref):
        self._table_ref = table_ref

    @classmethod
    def from_proto(
        cls, mysql_options_proto: DataSourceProto.CustomSourceOptions
    ) -> "MySQLOptions":
        options = json.loads(mysql_options_proto.configuration)
        mysql_options = cls(
            connection_str=options["connection_str"],
            table_ref=options["table_ref"],
        )
        return mysql_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        mysql_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {
                    "connection_str": self._connection_str,
                    "table_ref": self._table_ref,
                }
            ).encode("utf-8")
        )
        return mysql_options_proto


class MySQLServerSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        table_ref: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        **kwargs
    ):
        connection_str = get_offline_connection_str(project=kwargs["project"])
        self._mysql_options = MySQLOptions(connection_str, table_ref)
        self._connection_str = connection_str
        self._table_ref = table_ref
        _timestamp_field = timestamp_field or event_timestamp_column or ""
        super().__init__(
            timestamp_field=_timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            tags=tags,
            owner=owner,
            name=name,
            description=description,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, MySQLServerSource):
            raise TypeError(
                "Comparisons should only involve MySQLServerSource class objects."
            )

        return (
            self._mysql_options.connection_str
            == other._mysql_options.connection_str
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def connection_str(self):
        return self._mysql_options.connection_str

    @property
    def table_ref(self):
        return self._mysql_options.table_ref

    @property
    def mysql_options(self):
        return self._mysql_options

    @mysql_options.setter
    def mysql_options(self, _options):
        self._mysql_options = _options

    @staticmethod
    def from_proto(data_source: DataSourceProto, **kwargs) -> Any:
        options = json.loads(data_source.custom_options.configuration)
        return MySQLServerSource(
            field_mapping=dict(data_source.field_mapping),
            table_ref=options["table_ref"],
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
            **kwargs
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            field_mapping=self.field_mapping,
            custom_options=self._mysql_options.to_proto(),
        )
        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = (
            self.created_timestamp_column
        )
        data_source_proto.date_partition_column = self.date_partition_column
        return data_source_proto

    def validate(self, config: RepoConfig):
        # REVISIT(VK)
        return None

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return mysql_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        _connect_args = get_mysql_connect_args(self._connection_str)
        name_type_pairs = list()
        _db, _table = _connect_args["database"], self._table_ref
        _query = (
            f"select column_name, data_type from "
            f"INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '{_db}' "
            f"and TABLE_NAME = '{_table}'"
        )
        with connect(**_connect_args) as conn:
            table_schema = pd.read_sql(_query, conn)
            name_type_pairs.extend(
                list(
                    zip(
                        table_schema["COLUMN_NAME"].to_list(),
                        table_schema["DATA_TYPE"].to_list(),
                    )
                )
            )
        return name_type_pairs

    def get_table_query_string(self) -> str:
        return f"{self.table_ref}"


def mysql_to_feast_value_type(mysql_type_as_str: str) -> ValueType:
    _type_map = {
        "char": ValueType.STRING,
        "varchar": ValueType.STRING,
        "binary": ValueType.STRING,
        "varbinary": ValueType.STRING,
        "text": ValueType.STRING,
        "str": ValueType.STRING,
        "blob": ValueType.STRING,
        "mediumblob": ValueType.STRING,
        "longblob": ValueType.STRING,
        "tinyint": ValueType.INT32,
        "smallint": ValueType.INT32,
        "mediumint": ValueType.INT64,
        "bigint": ValueType.INT64,
        "int": ValueType.INT64,
        "float": ValueType.FLOAT,
        "double": ValueType.DOUBLE,
        "bool": ValueType.BOOL,
        "boolean": ValueType.INT32,
        "bytes": ValueType.BYTES,
        "timestamp": ValueType.UNIX_TIMESTAMP,
        "datetime": ValueType.UNIX_TIMESTAMP,
        "null": ValueType.NULL,
    }
    return _type_map[mysql_type_as_str]
