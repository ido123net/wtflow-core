from __future__ import annotations

from abc import ABC


class BaseService(ABC):
    def close(self) -> None:
        pass
