from uuid import UUID

from pydantic import BaseModel, NonNegativeInt, field_validator


class E1CMessage(BaseModel):
    class Properties(BaseModel, extra="allow"):
        integ_message_id: UUID
        integ_message_correlation_id: UUID
        SenderCode: str
        RecipientCode: list[str]
        integ_sender_code: str
        integ_recipient_code: list[str]
        integ_message_body_size: NonNegativeInt

    id: UUID
    delivery_tag: bytes
    delivery_number: NonNegativeInt
    durable: bool
    priority: NonNegativeInt
    properties: Properties
    body: bytes

    @field_validator("delivery_tag", mode="before")
    @classmethod
    def delivery_tag_validator(cls, v: str) -> bytes:
        return bytes.fromhex(v)
