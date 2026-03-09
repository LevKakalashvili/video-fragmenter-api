from pydantic import BaseModel


class Health(BaseModel):
    status: str
    extra: dict
