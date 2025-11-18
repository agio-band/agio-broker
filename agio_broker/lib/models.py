from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class Status(StrEnum):
    OK = "OK"
    PROCESSING = "PROCESSING"
    ERROR = "ERROR"


class ActionRequestModel(BaseModel):
    action: str
    args: list|None = None
    kwargs: dict|None = None
    workspace_id: UUID|None = None
    project_id: UUID|None = None


class ActionResponseModel(BaseModel):
    status: Status
    message: str = None
    data: Any = None
