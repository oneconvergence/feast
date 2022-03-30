from pathlib import Path
from decouple import AutoConfig

dconfig = AutoConfig(search_path=str(Path.home()))


def get_offline_store_conf():
    host = dconfig("OFFLINE_HOST")
    user = dconfig("OFFLINE_USER")
    port = dconfig("OFFLINE_PORT")
    password = dconfig("OFFLINE_SECRET")
    db = dconfig("OFFLINE_DB")
    return {
        "user": user,
        "host": host,
        "port": port,
        "password": password,
        "db": db
    }

def get_mysql_connect_args(connection_str):
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

def get_mysql_url(_connect_args):
    if not _connect_args:
        _connect_args = get_offline_store_conf()
    return f"""mysql+pymysql://{_connect_args['user']}:{
        _connect_args['password']}@{_connect_args['host']}:{
        _connect_args['port']}/{_connect_args['database']}"""


def get_offline_connection_str():
    offline_conf = get_offline_store_conf()
    return f"""{offline_conf['host']}:{offline_conf['port']}:
            {offline_conf['user']}@{offline_conf['password']}:
            {offline_conf['db']}"""


def get_dkube_server_config():
    return {
        "host": dconfig("ONLINE_SERVER_HOST"),
        "port": dconfig("ONLINE_SERVER_PORT")
    }

def get_dkube_db_config():
    return {
        "host": dconfig("DKUBE_DB"),
        "port": dconfig("DKUBE_DB_PORT"),
        "user": dconfig("DKUBE_DB_USER"),
        "secret": dconfig("DKUBE_DB_SECRET"),
        "db": dconfig("DKUBE_DBSTORE")
    }

def get_registry_config():
    return {
        # "ip": dconfig("DKUBE_IP"),
        "url": dconfig("DKUBE_URL"),
        "token": dconfig("DKUBE_TOKEN")
    }
