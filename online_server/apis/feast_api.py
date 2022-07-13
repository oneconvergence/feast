from datetime import datetime
import json
import os
import traceback
import fastapi
from drivers import mysql_driver
from models.online_db import InfraDelete, InfraUpdate, Materialize, MaterializeIncremental
from models.exceptions import ValidationError

router = fastapi.APIRouter()


@router.get("/ping", name="ping", status_code=200)
def ping():
    return {"message": "pong"}


@router.post("/api/v1/materialize", name="materialize", status_code=201)
async def materialize(materialize_input: Materialize) -> None:
    try:
        set_env(
            materialize_input.project, materialize_input.user, materialize_input.offline_dataset)
        mysql_driver.materialize(
            materialize_input.project,
            materialize_input.start_date,
            materialize_input.end_date,
            materialize_input.feature_views,
            materialize_input.user,
            materialize_input.offline_dataset
        )
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as ex:
        return fastapi.Response(content=str(ex), status_code=500)
    finally:
        unset_env(materialize_input.project)


@router.post("/api/v1/materialize_incr", name="materialize_incremental", status_code=201)
async def materialize_incremental(materialize_input: MaterializeIncremental) -> None:
    try:
        set_env(
            materialize_input.project, materialize_input.user, materialize_input.offline_dataset)
        mysql_driver.materialize_incremental(
            materialize_input.project,
            materialize_input.end_date,
            materialize_input.feature_views,
            materialize_input.user,
            materialize_input.offline_dataset
        )
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as ex:
        print(traceback.format_exc())
        return fastapi.Response(content=str(ex), status_code=500)
    finally:
        unset_env(materialize_input.project)


@router.post("/api/v1/infra_update", name="infra_update", status_code=201)
async def update_infra(infra_input: InfraUpdate) -> None:
    try:
        set_env(
            infra_input.project, infra_input.user, infra_input.offline_dataset)
        mysql_driver.infra_update(
            infra_input.project,
            infra_input.tables_to_keep,
            infra_input.tables_to_delete,
            infra_input.entities_to_keep,
            infra_input.entities_to_delete,
            infra_input.user,
            infra_input.offline_dataset
        )
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as ex:
        return fastapi.Response(content=str(ex), status_code=500)
    finally:
        unset_env(infra_input.project)


@router.delete("/api/v1/teardown", name="teardown", status_code=200)
async def teardown_infra(infra_delete: InfraDelete) -> None:
    try:
        mysql_driver.teardown(infra_delete.tables, infra_delete.entities, infra_delete.user)
    except ValidationError as ve:
        return fastapi.Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as ex:
        print(traceback.format_exc())
        return fastapi.Response(content=str(ex), status_code=500)


def set_env(project, user, offline_dataset):
    os.environ[project] = f"{user}_{offline_dataset}"


def unset_env(project):
    os.environ.pop(project, None)


# if __name__ == "__main__":
#     json_data = {
#         "start_date": datetime.now().isoformat(),
#         "end_date": datetime.now().isoformat(),
#     }
#     serialized_data = json.dumps(json_data)
#     print(serialized_data)
#     m = Materialize.parse_raw(serialized_data)
#     print("m: ", m)
