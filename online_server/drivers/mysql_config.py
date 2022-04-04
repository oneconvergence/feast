from typing import Any, Dict, Optional

from common.base import Base
from provider.sdk.dkubefs.utils import get_dkube_db_config


class MysqlConfig(metaclass=Base):
    _CONFIG: Optional[Dict[str, Any]] = None

    def __init__(self) -> None:
        if not self._CONFIG:
            online_store = get_dkube_db_config()
            self._CONFIG = {
                "host": online_store["host"],
                "port": online_store["port"],
                "user": online_store["user"],
                "password": online_store["secret"],
                "database": online_store["db"],
                "autocommit": True,
            }

    @staticmethod
    def get_config():
        return MysqlConfig._CONFIG
