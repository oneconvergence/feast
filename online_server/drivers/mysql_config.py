from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from common.base import Base


class MysqlConfig(metaclass=Base):
    _CONFIG: Optional[Dict[str, Any]] = None

    def __init__(self) -> None:
        if not self._CONFIG:
            with open(str(Path("feature_store.yaml").absolute()), "r") as f:
                data = yaml.safe_load(f)
                online_store = data["online_store"]
                self._CONFIG = {
                    "host": online_store["host"],
                    "port": online_store["port"],
                    "user": online_store["user"],
                    "password": online_store["password"],
                    "database": online_store["db"],
                    # "db": online_store["db"],
                    "autocommit": True,
                }

    @staticmethod
    def get_config():
        return MysqlConfig._CONFIG
