from pydantic import BaseModel


class ActionResponseModel(BaseModel):
    ok: bool
    message: str = None
    retry_later: int = None
    payload: dict = None