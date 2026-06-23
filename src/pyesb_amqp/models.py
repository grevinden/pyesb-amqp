from uuid import UUID

from pydantic import BaseModel, Field, NonNegativeInt, field_validator


class E1CMessage(BaseModel):
    class Properties(BaseModel, extra="allow"):
        integ_message_id: UUID
        integ_message_correlation_id: UUID
        sender_code: str = Field(validation_alias="SenderCode")
        recipient_code: list[str] = Field(validation_alias="RecipientCode")
        integ_sender_code: str
        integ_recipient_code: list[str]
        integ_message_body_size: NonNegativeInt

        @field_validator("recipient_code", mode="before")
        @classmethod
        def recipient_code_validator(cls, v: str) -> list[str]:
            return v.split(sep=",")

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
