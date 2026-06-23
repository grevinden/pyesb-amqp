from uuid import UUID

from pydantic import BaseModel, Field, NonNegativeInt


class E1CMessage(BaseModel):
    id: UUID
    delivery_tag: NonNegativeInt = Field(validation_alias="delivery_number")
    durable: bool
    priority: NonNegativeInt
    properties: dict[str, str]
    body: bytes
