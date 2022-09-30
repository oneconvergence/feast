import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional
from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jinja2
import yaml


USERS = {}
token_auth_scheme = HTTPBearer()


def get_repo_path() -> str:
    repo_path = Path("online_repo/").absolute()
    repo_path_str = str(repo_path)
    return repo_path_str


def gen_template(
    input_file: str, output_file: str, template_file: Optional[str] = None
):
    if input_file.endswith((".yml", ".yaml")):
        with open(input_file, "r") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
    elif input_file.endswith(".json"):
        with open(input_file, "r") as f:
            data = json.loads(f.read())
    else:
        sys.exit(f"input_file {input_file} type not supported.")

    if template_file and template_file.suffix != ".j2":
        sys.exit(f"Template file {template_file} format not supported")

    if not template_file:
        template_file = Path("common/templates/feast_repo.j2").absolute()
        template_file = str(template_file)

    with open(template_file, "r") as tfile:
        template_data = tfile.read()

    template = jinja2.Template(template_data, keep_trailing_newline=True)
    config_data = template.render({"input": data})

    online_repo = Path("online_repo/").absolute()
    output_file = f"{str(online_repo)}/{output_file}"

    with open(output_file, "w") as of:
        of.write(config_data)

    print(f"Generated o/p file: {output_file}")


def add_user_info(user: str, val: dict):
    global USERS
    USERS[user] = val


def get_user_info(user: str) -> dict:
    global USERS
    if user in USERS:
        user_info = USERS[user]
    else:
        user_info = {user: {}}
    return user_info


def get_user_info_by_project(project: str) -> dict:
    global USERS
    for user, user_val in USERS.items():
        if user_val["project"] == project:
            return {
                "user": user,
                **USERS[user]
            }
    return {}


def del_user_info(user: str):
    global USERS
    if user in USERS:
        del USERS[user]


def list_user_info() -> Dict[str, Dict[Any, Any]]:
    global USERS
    return USERS


def set_env(project, user, offline_dataset):
    pass


def unset_env(project):
    pass


async def extract_info(
    request: Request,
    token: HTTPAuthorizationCredentials = Depends(token_auth_scheme),
):
    try:
        req_json = await request.json()
    except json.decoder.JSONDecodeError:
        req_json = {}
    user = req_json.get("user")
    offline_dataset = req_json.get("offline_dataset")
    project = req_json.get("project")
    user_token = token.credentials
    if user:
        add_user_info(
            user, {
                "offline_dataset": offline_dataset,
                "token": user_token,
                "project": project
            }
        )


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit("Usage: python <input file> <output file>")
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    gen_template(input_file, output_file)
