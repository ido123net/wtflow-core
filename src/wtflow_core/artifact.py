from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Artifact:
    name: str
    file_type: str = "txt"
