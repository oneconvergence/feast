from typing import Any


class Base(type):
    _instances = {}

    def __call__(cls, *args: Any, **kwds: Any) -> None:
        if cls not in cls._instances:
            cls._instances[cls] = super(Base, cls).__call__(*args, **kwds)
        return cls._instances[cls]
