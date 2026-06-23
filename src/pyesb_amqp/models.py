from uuid import UUID

from pydantic import BaseModel, NonNegativeInt


class E1CMessage(BaseModel):
    id: UUID
    delivery_tag: str
    durable: bool
    priority: NonNegativeInt
    properties: dict[str, str]
    body: bytes
