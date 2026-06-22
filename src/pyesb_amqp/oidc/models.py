from typing import Literal
from uuid import UUID

from pydantic import Field, NonNegativeInt, PositiveInt
from pydantic.main import BaseModel


class ChannelBase(BaseModel):
    process: str = "pyesb"
    channel: str


class ChannelMetadata(ChannelBase):
    process_description: str = Field(
        "Python ESB", serialization_alias="processDescription"
    )
    channel_description: str = Field(..., serialization_alias="channelDescription")
    access: Literal["READ_ONLY", "WRITE_ONLY"]


class ChannelRuntime(BaseModel):
    class Metadata(ChannelBase):
        destination: str

    items: list[Metadata]
    port: PositiveInt = 6698


class E1CMessage(BaseModel):
    id: UUID
    durable: bool
    priority: NonNegativeInt
    properties: dict[str, str]
    body: str


class Token(BaseModel):
    id_token: Literal[None] = None
    access_token: Literal["Not implemented"] = "Not implemented"
    token_type: Literal["Bearer"] = "Bearer"
