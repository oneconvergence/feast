from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow
from decouple import AutoConfig
from feast import OnDemandFeatureView, errors
from feast.data_source import DataSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.provider import _get_requested_feature_views_to_features_dict
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from mysql.connector import connect
from pydantic.types import StrictStr
from pydantic.typing import Literal
from sqlalchemy import create_engine
from provider.sdk.dkubefs.mysqlserver_source import MySQLServerSource
from provider.sdk.dkubefs.utils import get_mysql_connect_args, \
     get_mysql_url


dconfig = AutoConfig(search_path=str(Path.home()))
EntitySchema = Dict[str, np.dtype]


class MySQLOfflineStoreConfig(FeastConfigBaseModel):
    type: Literal["dkubefs.mysqlserver.MySQLOfflineStore"]


class MySQLOfflineStore(OfflineStore):
    def __init__(self):
        self.mysql_connect_args = None

    def _get_mysql_connect_config(self, config: RepoConfig) -> Dict:
        if not self.mysql_connect_args:
            self.mysql_connect_args = get_mysql_connect_args()
        return self.mysql_connect_args

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert type(data_source).__name__ == "MySQLServerSource"
        assert config.offline_store.type == (
            "dkube.mysqlserver.MySQLOfflineStore"
        )
        from_expression = data_source.get_table_query_string()

        columns_join_string = ", ".join(join_key_columns)
        if columns_join_string != "":
            columns_join_string = "PARTITION BY " + columns_join_string
        timestamps = [event_timestamp_column]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)

        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamps
        )

        splitted_fields = field_string.split(',')
        splitted_fields = [x.strip() for x in splitted_fields]
        if 'index' in splitted_fields:
            splitted_fields.remove('index')
            splitted_fields.insert(0, '`index`')
            field_string = ', '.join(splitted_fields)

        query = f"""
            SELECT
                {field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}"
                    if not join_key_columns else ""}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER(
                {columns_join_string} ORDER BY {timestamp_desc_string}
                ) AS _feast_row
                FROM {from_expression}
                WHERE {event_timestamp_column}
                BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            ) as tt
            WHERE _feast_row = 1
            """

        # call retrieval function
        return MySQLRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    # REVISIT(VK): We may need to revisit this.
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, MySQLOfflineStoreConfig)

        # REVISIT(VK): Do we need to bring our own definition ?
        expected_join_keys = offline_utils.get_expected_join_keys(
            project, feature_views, registry
        )

        (
            table_schema,
            table_name,
        ) = _upload_entity_df_into_mysql_and_get_entity_schema(
            config=config, entity_df=entity_df
        )

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(table_schema)
        )

        offline_utils.assert_expected_columns_in_entity_df(
            table_schema, expected_join_keys, entity_df_event_timestamp_col
        )

        query_context = get_feature_view_query_context(
            feature_refs, feature_views, registry, project
        )

        query = offline_utils.build_point_in_time_query(
            query_context,
            left_table_query_string=table_name,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
            entity_df_columns=table_schema.keys()
        )

        job = MySQLRetrievalJob(
            query=query,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=registry.list_on_demand_feature_views(
                project
            ),
        )
        return job


@dataclass(frozen=True)
class FeatureViewQueryContext:
    """Context object used to template a point-in-time MySQL query"""

    name: str
    ttl: int
    entities: List[str]
    features: List[str]  # feature reference format
    event_timestamp_column: str
    created_timestamp_column: Optional[str]
    table_subquery: str
    entity_selections: List[str]
    table_ref: str


def _upload_entity_df_into_mysql_and_get_entity_schema(
    config: RepoConfig,
    entity_df: Union[pd.DataFrame, str],
) -> Tuple[EntitySchema, str]:
    """
    Uploads a Pandas entity dataframe into MySQL table and constructs the
    schema from the original entity_df dataframe.
    """
    table_id = offline_utils.get_temp_entity_table_name()
    offline_config = config.offline_store
    _connect_args = get_mysql_connect_args(offline_config.connection_str)
    _mysql_url = get_mysql_url(_connect_args)
    engine = create_engine(_mysql_url)
    if type(entity_df) is str:
        _query = f"create table {table_id} as {entity_df}"
        with engine.connect() as conn:
            conn.execute(_query)

        retrieved_entity_df = MySQLRetrievalJob(
            f"SELECT * FROM {table_id} LIMIT 1",
            config,
            full_feature_names=False,
            on_demand_feature_views=None,
        ).to_df()  # to_df - defined in RetrievalJob
        return (
            dict(zip(retrieved_entity_df.columns, retrieved_entity_df.dtypes)),
            table_id,
        )

    elif type(entity_df) is pd.DataFrame:
        # _mysql_url = get_mysql_url(_connect_args)
        # engine = create_engine(_mysql_url)
        with engine.connect() as conn:
            entity_df.to_sql(name=table_id, con=conn, if_exists="replace")
        return dict(zip(entity_df.columns, entity_df.dtypes)), table_id
    raise Exception("Unsupported entitydf type")

def _assert_expected_columns_in_mysql(
    join_keys: Set[str],
    entity_df_event_timestamp_col: str,
    table_schema: EntitySchema,
):
    entity_columns = set(table_schema.keys())

    expected_columns = join_keys.copy()
    expected_columns.add(entity_df_event_timestamp_col)

    missing_keys = expected_columns - entity_columns

    if len(missing_keys) != 0:
        raise errors.FeastEntityDFMissingColumnsError(
            expected_columns, missing_keys
        )


def get_feature_view_query_context(
    feature_refs: List[str],
    feature_views: List[FeatureView],
    registry: Registry,
    project: str,
) -> List[FeatureViewQueryContext]:
    """Build a query context containing all information required to template
    a point-in-time SQL query
    """

    (
        feature_views_to_feature_map,
        on_demand_feature_views_to_features,
    ) = _get_requested_feature_views_to_features_dict(
        feature_refs,
        feature_views,
        registry.list_on_demand_feature_views(project),
    )

    query_context = []
    for feature_view, features in feature_views_to_feature_map.items():
        join_keys = []
        entity_selections = []
        reverse_field_mapping = {
            v: k for k, v in feature_view.input.field_mapping.items()
        }
        for entity_name in feature_view.entities:
            entity = registry.get_entity(entity_name, project)
            join_key = feature_view.projection.join_key_map.get(
                entity.join_key, entity.join_key
            )
            join_keys.append(join_key)
            entity_selections.append(f"{entity.join_key} AS {join_key}")

        if isinstance(feature_view.ttl, timedelta):
            ttl_seconds = int(feature_view.ttl.total_seconds())
        else:
            ttl_seconds = 0

        assert isinstance(feature_view.input, MySQLServerSource)
        event_timestamp_column = feature_view.input.event_timestamp_column
        created_timestamp_column = feature_view.input.created_timestamp_column

        context = FeatureViewQueryContext(
            name=feature_view.projection.name_to_use(),
            ttl=ttl_seconds,
            entities=join_keys,
            features=features,
            event_timestamp_column=reverse_field_mapping.get(
                event_timestamp_column, event_timestamp_column
            ),
            created_timestamp_column=reverse_field_mapping.get(
                created_timestamp_column, created_timestamp_column
            ),
            # TODO: Make created column optional and not hardcoded
            table_subquery=feature_view.input.get_table_query_string(),
            entity_selections=entity_selections,
            table_ref=feature_view.input.table_ref,
        )
        query_context.append(context)
    return query_context


class MySQLRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: StrictStr,
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]],
        drop_columns: Optional[List[str]] = None,
    ):
        self.query = query
        self._config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views
        self._drop_columns = drop_columns
        self._connect_args = get_mysql_connect_args()
        _mysql_url = get_mysql_url(self._connect_args)
        self.engine = create_engine(_mysql_url)

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    def _to_df_internal(self) -> pd.DataFrame:
        # with connect(**self._connect_args) as conn:
        with self.engine.connect() as conn:
            df = pd.read_sql(sql=self.query, con=conn).fillna(value=np.nan)
            return df

    def _to_arrow_internal(self) -> pyarrow.Table:
        # with connect(**self._connect_args) as conn:
        with self.engine.connect() as conn:
            df = pd.read_sql(sql=self.query, con=conn).fillna(value=np.nan)
            return pyarrow.Table.from_pandas(df=df)


# REVISIT(VK): This template is in sync with redshift.py. Changes have been done to make it compatible with mysql. For time being we need to keep sync this with bigquery.py and redshift.py
MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST({{entity}} as CHAR(100)) ||
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS CHAR(50))
            ) AS {{featureview.name}}__entity_row_unique_id
            {% else %}
            ,CAST({{entity_df_event_timestamp_col}} AS CHAR(100)) AS {{featureview.name}}__entity_row_unique_id
            {% endif %}
        {% endfor %}
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

{{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY
        {{ featureview.entities | join(', ')}}{% if featureview.entities %},{% else %}{% endif %}
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.event_timestamp_column }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} AS sub
    WHERE {{ featureview.event_timestamp_column }} <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.event_timestamp_column }} >= (SELECT MIN(entity_timestamp) FROM entity_dataframe) - interval {{ featureview.ttl }} second
    {% endif %}
),

{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - interval {{ featureview.ttl }} second
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
/*
{{ featureview.name }}__latest AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        MAX(event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,MAX(created_timestamp) AS created_timestamp
        {% endif %}

    FROM {{ featureview.name }}__base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup
        USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
    {% endif %}

    GROUP BY {{featureview.name}}__entity_row_unique_id
),
*/

{{ featureview.name }}__latest AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        {{featureview.name}}__entity_row_unique_id
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY {{featureview.name}}__entity_row_unique_id
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS row_num
        FROM {{ featureview.name }}__base
        {% if featureview.created_timestamp_column %}
            INNER JOIN {{ featureview.name }}__dedup
            USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
        {% endif %}
    ) AS sub
    WHERE row_num = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT *
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) as tmp USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
