import os
import sys
from pathlib import Path

from decouple import AutoConfig
from dkube.sdk import DkubeApi
from online_server.common.utils.utils import get_user_info

dconfig = AutoConfig(search_path=str(Path.home()))


def get_dkube_client(token: str):
    reg_conf = get_registry_config()
    DKUBE_URL = os.getenv("DKUBE_URL", reg_conf["url"])
    if not DKUBE_URL:
        sys.exit("Dkube access url not set.")
    # DKUBE_TOKEN = os.getenv("DKUBE_USER_ACCESS_TOKEN", reg_conf["token"])
    DKUBE_TOKEN = token
    if not DKUBE_TOKEN:
        sys.exit("Dkube access token not set.")
    dkube = DkubeApi(URL=DKUBE_URL, token=DKUBE_TOKEN)
    return dkube


def get_offline_store_conf(offline_user=None, offline_dataset=None):
    if offline_user:
        USER = offline_user
    else:
        USER = os.getenv("DKUBE_USER_LOGIN_NAME")
        if not USER:
            raise Exception("dkube user login name not specified.")
    if offline_dataset:
        offline_ds = offline_dataset
    else:
        offline_ds = os.getenv("OFFLINE_DATASET")
        if not offline_ds:
            raise Exception("offline dataset information not specified.")

    token = get_user_token(USER)
    dclient = get_dkube_client(token)
    ods = dclient.get_dataset(USER, offline_ds)
    return {
        "user": ods["datum"]["sql"]["username"],
        "host": ods["datum"]["sql"]["host"],
        "port": ods["datum"]["sql"]["port"],
        "password": ods["datum"]["sql"]["password"],
        "db": ods["datum"]["sql"]["database"],
        "autocommit": True,
    }


def get_mysql_connect_args(connection_str=None):
    if connection_str:
        mysql_config = connection_str.split(":")
        mysql_ip = mysql_config[0]
        mysql_port = mysql_config[1]
        _creds = mysql_config[2].split("@")
        _user = _creds[0]
        _password = _creds[1]
        db = mysql_config[3]
        conf = {
            "host": mysql_ip,
            "port": mysql_port,
            "user": _user,
            "password": _password,
            "database": db,
        }
    else:
        conf = get_offline_store_conf()
    conf.update(autocommit=True)
    return conf


def get_offline_connection_str(user=None, offline_dataset=None):
    offline_conf = get_offline_store_conf(user, offline_dataset)
    return f"""{offline_conf['host']}:{offline_conf['port']}:
            {offline_conf['user']}@{offline_conf['password']}:
            {offline_conf['db']}"""


def get_dkube_server_config():
    feast_ol_url = os.getenv("FEAST_ONLINE_SERVER_URL")
    if feast_ol_url:
        return feast_ol_url
    elif dconfig("FEAST_ONLINE_SERVER_URL", default=None):
        return dconfig("FEAST_ONLINE_SERVER_URL", default=None)
    else:
        print("Using default server config.")
        return "http://knative-local-gateway.istio-system.svc.cluster.local"


def get_dkube_server_host():
    feast_ol_url = os.getenv("FEAST_ONLINE_SERVER_HOST")
    if feast_ol_url:
        return feast_ol_url
    else:
        return {"Host": "feast-online-server.default.svc"}


def get_mysql_url(_connect_args=None, user=None, offline_dataset=None):
    if not _connect_args:
        if user and offline_dataset:
            _connect_args = get_offline_store_conf(user, offline_dataset)
        else:
            raise Exception("MYSQL url cannot be retreived.")
    if "db" in _connect_args:
        db = _connect_args["db"]
    elif "database" in _connect_args:
        db = _connect_args["database"]
    else:
        sys.exit("db or database not set")
    return f"""mysql+pymysql://{_connect_args['user']}:{
        _connect_args['password']}@{_connect_args['host']}:{
        _connect_args['port']}/{db}"""


def get_dkube_db_config(user):
    dds = os.getenv("ONLINE_DATASET", dconfig("ONLINE_DATASET", default=None))
    if not dds:
        print("online dataset not found. using default: online-dataset")
        dds = "online-dataset"
        print("using default dataset: online-dataset")

    token = get_user_token(user)
    dclient = get_dkube_client(token)
    ods = dclient.get_dataset(user, dds)
    return {
        "host": ods["datum"]["sql"]["host"],
        "port": ods["datum"]["sql"]["port"],
        "user": ods["datum"]["sql"]["username"],
        "password": ods["datum"]["sql"]["password"],
        "db": ods["datum"]["sql"]["database"],
        "autocommit": True,
    }


def get_registry_config():
    dkube_url = os.getenv("DKUBE_URL", dconfig("DKUBE_URL", default=None))
    if not dkube_url:
        sys.exit("DKUBE_URL not set.")

    # dkube_token = os.getenv(
    #     "DKUBE_USER_ACCESS_TOKEN",
    #     dconfig("DKUBE_USER_ACCESS_TOKEN", default=None),
    # )
    # if not dkube_token:
    #     sys.exit("DKUBE_USER_ACCESS_TOKEN not set.")

    return {"url": dkube_url}


def get_user_token(user: str):
    user_info = get_user_info(user)
    if not user_info:
        raise Exception("User details not found.")
    if "token" not in user_info[user]:
        raise Exception("User token not found.")
    return user_info[user]["token"]
