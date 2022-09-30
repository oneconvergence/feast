import traceback
from pathlib import Path

from drivers import mysql_driver
from fastapi import APIRouter, Depends, Response
from feast.repo_config import load_repo_config
from feast.repo_operations import cli_check_repo, registry_dump
from models.exceptions import ValidationError
from models.online_db import (
    InfraDelete,
    InfraUpdate,
    Materialize,
    MaterializeIncremental,
)
from online_server.common.utils.utils import (
    extract_info,
    list_user_info,
)

router = APIRouter()


@router.get("/ping", name="ping", status_code=200)
def ping():
    return {"message": "pong"}


@router.post(
    "/api/v1/materialize",
    name="materialize",
    status_code=201,
    dependencies=[Depends(extract_info)],
)
async def materialize(materialize_input: Materialize) -> None:
    try:
        mysql_driver.materialize(
            materialize_input.project,
            materialize_input.start_date,
            materialize_input.end_date,
            materialize_input.feature_views,
            materialize_input.user,
            materialize_input.offline_dataset,
        )
    except ValidationError as ve:
        return Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as ex:
        return Response(content=str(ex), status_code=500)


@router.post(
    "/api/v1/materialize_incr",
    name="materialize_incremental",
    status_code=201,
    dependencies=[Depends(extract_info)],
)
async def materialize_incremental(
    materialize_input: MaterializeIncremental,
) -> None:
    try:
        mysql_driver.materialize_incremental(
            materialize_input.project,
            materialize_input.end_date,
            materialize_input.feature_views,
            materialize_input.user,
            materialize_input.offline_dataset,
        )
    except ValidationError as ve:
        return Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as ex:
        print(traceback.format_exc())
        return Response(content=str(ex), status_code=500)


@router.post(
    "/api/v1/infra_update",
    name="infra_update",
    status_code=201,
    dependencies=[Depends(extract_info)],
)
async def update_infra(infra_input: InfraUpdate) -> None:
    try:
        mysql_driver.infra_update(
            infra_input.project,
            infra_input.tables_to_keep,
            infra_input.tables_to_delete,
            infra_input.entities_to_keep,
            infra_input.entities_to_delete,
            infra_input.user,
            infra_input.offline_dataset,
        )
    except ValidationError as ve:
        return Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as ex:
        print(traceback.format_exc())
        return Response(content=str(ex), status_code=500)


@router.delete(
    "/api/v1/teardown",
    name="teardown",
    status_code=200,
    dependencies=[Depends(extract_info)],
)
async def teardown_infra(infra_delete: InfraDelete) -> None:
    try:
        mysql_driver.teardown(
            infra_delete.tables, infra_delete.entities, infra_delete.user
        )
    except ValidationError as ve:
        return Response(content=ve.error_msg, status_code=ve.status_code)
    except Exception as ex:
        print(traceback.format_exc())
        return Response(content=str(ex), status_code=500)


@router.get(
    "/api/v1/registry/{project}",
    name="registry",
    status_code=200,
    dependencies=[Depends(extract_info)],
)
async def registry_details(project: str):
    repo = str(Path().absolute().parent) + "/online_repo"
    cli_check_repo(repo)
    repo_config = load_repo_config(repo)
    repo_config.project = project
    dump = registry_dump(repo_config, repo_path=repo)
    return dump


@router.get(
    "/api/v1/user_info", status_code=200, dependencies=[Depends(extract_info)]
)
async def dump_user_info():
    return list_user_info()


# if __name__ == "__main__":
#     json_data = {
#         "start_date": datetime.now().isoformat(),
#         "end_date": datetime.now().isoformat(),
#     }
#     serialized_data = json.dumps(json_data)
#     print(serialized_data)
#     m = Materialize.parse_raw(serialized_data)
#     print("m: ", m)
