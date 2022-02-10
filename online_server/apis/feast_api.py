from datetime import datetime
from typing import List, Optional, Sequence

import fastapi
from drivers import mysql_driver
from pydantic import BaseModel

router = fastapi.APIRouter()


class Materialize(BaseModel):
    start_date: datetime
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
    entities: Sequence[str]


@router.post("/api/v1/materialize", name="materialize", status_code=201)
def materialize(materialize_input: Materialize) -> None:
    mysql_driver.materialize(
        materialize_input.start_date,
        materialize_input.end_date,
        materialize_input.feature_views,
    )


@router.post("/api/v1/infra_update", name="infra_update", status_code=201)
def update_infra(infra_input: InfraUpdate) -> None:
    mysql_driver.infra_update(
        infra_input.project,
        infra_input.tables_to_keep,
        infra_input.tables_to_delete,
        infra_input.entities_to_keep,
        infra_input.entities_to_delete,
    )


@router.post("/api/v1/teardown", name="teardown", status_code=201)
def teardown_infra(infra_delete: InfraDelete) -> None:
    mysql_driver.teardown(infra_delete.tables, infra_delete.entities)


# if __name__ == "__main__":
#     json_data = {
#         "start_date": datetime.now().isoformat(),
#         "end_date": datetime.now().isoformat(),
#     }
#     serialized_data = json.dumps(json_data)
#     print(serialized_data)
#     m = Materialize.parse_raw(serialized_data)
#     print("m: ", m)
