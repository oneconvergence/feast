import os
from pathlib import Path
import sys
import tomli

from dkube.sdk import DkubeApi


def get_cofiguration():
    _path = Path("/etc/feast_cfg.toml")
    try:
        with open(_path, "rb") as _cfg:
            _config = tomli.load(_cfg)
            return _config
    except FileNotFoundError as fe:
        print(f"config file not found: {fe}")
        return {}


dconfig = get_cofiguration()


def get_dkube_client():
    reg_conf = get_registry_config()
    DKUBE_URL = os.getenv("DKUBE_URL", reg_conf["url"])
    if not DKUBE_URL:
        sys.exit("Dkube access url not set.")
    DKUBE_TOKEN = os.getenv("DKUBE_USER_ACCESS_TOKEN", reg_conf["token"])
    if not DKUBE_TOKEN:
        sys.exit("Dkube access token not set.")
    dkube = DkubeApi(URL=DKUBE_URL, token=DKUBE_TOKEN)
    return dkube


def get_offline_store_conf(offline_user=None):
    if offline_user:
        USER = offline_user
    else:
        if os.getenv("DKUBE_USER_LOGIN_NAME"):
            USER = os.getenv("DKUBE_USER_LOGIN_NAME")
        else:
            sys.exit("Please specify dkube user name in DKUBE_USER_LOGIN_NAME "
                    "environment variable.")
    offline_ds = os.getenv("OFFLINE_DATASET", dconfig.get("OFFLINE_DATASET"))
    if offline_ds:
        dclient = get_dkube_client()
        ods = dclient.get_dataset(USER, offline_ds)
        return {
            "user": ods["datum"]["sql"]["username"],
            "host": ods["datum"]["sql"]["host"],
            "port": ods["datum"]["sql"]["port"],
            "password": ods["datum"]["sql"]["password"],
            "db": ods["datum"]["sql"]["database"]
        }
    # Notes(VK): We can get away with this.
    offline_server = dconfig.get("offline_server")
    if not offline_server:
        sys.exit("Offline server details not provided in config file.")
    host = offline_server["host"]
    user = offline_server["user"]
    port = offline_server["port"]
    password = offline_server["password"]
    db = offline_server["db"]

    if not all([host, user, port, password, db]):
        sys.exit("Offline server details not found. Please "
                 "specify OFFLINE_DATASET in env variable or"
                 "offline server details individually in config"
                 "file.")

    return {
        "user": user,
        "host": host,
        "port": port,
        "password": password,
        "db": db
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

def get_mysql_url(_connect_args=None):
    if not _connect_args:
        _connect_args = get_offline_store_conf()
    if 'db' in _connect_args:
        db = _connect_args['db']
    elif 'database' in _connect_args:
        db = _connect_args['database']
    else:
        sys.exit("db or database not set")
    return f"""mysql+pymysql://{_connect_args['user']}:{
        _connect_args['password']}@{_connect_args['host']}:{
        _connect_args['port']}/{db}"""


def get_offline_connection_str():
    offline_conf = get_offline_store_conf()
    return f"""{offline_conf['host']}:{offline_conf['port']}:
            {offline_conf['user']}@{offline_conf['password']}:
            {offline_conf['db']}"""


def get_dkube_server_config():
    feast_ol_url = os.getenv("FEAST_ONLINE_SERVER_URL")
    if feast_ol_url:
        return feast_ol_url
    elif dconfig.get("FEAST_ONLINE_SERVER_URL"):
        return dconfig["FEAST_ONLINE_SERVER_URL"]
    else:
        print("Using default server config.")
        return "http://knative-local-gateway.istio-system.svc.cluster.local"


def get_dkube_server_host():
    feast_ol_url = os.getenv("FEAST_ONLINE_SERVER_HOST")
    if feast_ol_url:
        return feast_ol_url
    else:
        return {
            "Host": "feast-online-server.default.svc"
        }


def get_dkube_db_config():
    dds = os.getenv("ONLINE_DATASET", dconfig.get("ONLINE_DATASET"))
    if not dds:
        print("online dataset not found. using default: online-dataset")
        dds = "online-dataset"
    dclient = get_dkube_client()
    ods = dclient.get_dataset("ocdkube", dds)
    return {
        "host": ods["datum"]["sql"]["host"],
        "port": ods["datum"]["sql"]["port"],
        "user": ods["datum"]["sql"]["username"],
        "secret": ods["datum"]["sql"]["password"],
        "db": ods["datum"]["sql"]["database"]
    }


def get_registry_config():
    dkube_url = os.getenv("DKUBE_URL", dconfig.get("DKUBE_URL"))
    if not dkube_url:
        # if "url" not in dconfig.get("dkube", {}):
        sys.exit("DKUBE_URL not set.")

    dkube_token = os.getenv("DKUBE_USER_ACCESS_TOKEN",
                    dconfig.get("DKUBE_USER_ACCESS_TOKEN"))
    if not dkube_token:
        sys.exit("DKUBE_USER_ACCESS_TOKEN not set.")

    return {
        "url": dkube_url,
        "token": dkube_token
    }


def get_user():
    user = os.getenv("DKUBE_USER_LOGIN_NAME",
            dconfig.get("DKUBE_USER_LOGIN_NAME"))
    if not user:
        raise Exception("DKUBE_USER_LOGIN_NAME not specified.")
    return user


def get_offline_dataset():
    offline_ds = os.getenv("OFFLINE_DATASET",
                    dconfig.get("OFFLINE_DATASET"))
    if not offline_ds:
        raise Exception("OFFLINE_DATASET not specified.")
    return offline_ds
