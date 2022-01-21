def get_mysql_connect_args(connection_str):
    mysql_config = connection_str.split(":")
    mysql_ip = mysql_config[0]
    mysql_port = mysql_config[1]
    _creds = mysql_config[2].split("@")
    _user = _creds[0]
    _password = _creds[1]
    db = mysql_config[3]
    return {
        "host": mysql_ip,
        "port": mysql_port,
        "user": _user,
        "password": _password,
        "database": db,
        "autocommit": True,
    }

def get_mysql_url(_connect_args):
    return f"""mysql+pymysql://{_connect_args['user']}:{
        _connect_args['password']}@{_connect_args['host']}:{
        _connect_args['port']}/{_connect_args['database']}"""
