from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, NonNegativeInt, PositiveInt, field_validator


class E1CMessage(BaseModel, extra="allow"):
    class Properties(BaseModel, extra="allow"):
        message_id: UUID
        correlation_id: UUID | None = Field(None)
        absolute_expiry_time: datetime
        creation_time: datetime

    class ApplicationProperties(BaseModel, extra="allow"):
        integ_sender_code: str | None = Field(None)
        integ_recipient_code: list[str] | None = Field(None)
        integ_message_body_size: NonNegativeInt
        integ_message_correlation_id: UUID | None = Field(None)
        integ_message_id: UUID

        @field_validator("integ_recipient_code", mode="before")
        @classmethod
        def recipient_code_validator(cls, v: str) -> list[str]:
            return v.split(sep=",")

    class Header(BaseModel):
        delivery_count: NonNegativeInt
        first_acquirer: bool
        priority: NonNegativeInt
        durable: bool

    body: bytes
    delivery_annotations: Literal[None]
    delivery_id: NonNegativeInt
    delivery_tag: PositiveInt
    footer: Literal[None]
    header: Header
    link_output_handle: NonNegativeInt
    message_annotations: Literal[None]
    message_format: NonNegativeInt
    properties: Properties
    application_properties: ApplicationProperties
    rcv_settle_mode: Literal[None]

    @field_validator("delivery_tag", mode="before")
    @classmethod
    def delivery_tag_validator(cls, v: bytes) -> PositiveInt:
        return int.from_bytes(v, byteorder="little")
