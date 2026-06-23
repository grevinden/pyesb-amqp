from typing import Literal

from pydantic import Field, PositiveInt
from pydantic.main import BaseModel


class ProcessBase(BaseModel):
    process: str = Field("pyesb", pattern=r"[a-z0-9]{1-15}")


class ProcessModel(ProcessBase):
    process_description: str = Field(
        "Python ESB", serialization_alias="processDescription", max_length=100
    )


class ChannelBase(BaseModel):
    channel: str = Field("pyesb", pattern=r"[a-z0-9]{1-15}")


class ChannelModel(ChannelBase):
    channel_description: str = Field(
        ..., serialization_alias="channelDescription", max_length=100
    )


class ChannelMetadata(ProcessModel, ChannelModel):
    access: Literal["READ_ONLY", "WRITE_ONLY"] = "WRITE_ONLY"


class ChannelRuntime(BaseModel):
    class Metadata(ProcessBase, ChannelBase):
        destination: str = Field(pattern=r"[a-z0-9]{1-15}")

    items: list[Metadata]
    port: PositiveInt = 6698


class ChannelDesription(ChannelMetadata, ChannelRuntime.Metadata): ...


class Token(BaseModel):
    id_token: Literal[None] = None
    access_token: Literal["Not implemented"] = "Not implemented"
    token_type: Literal["Bearer"] = "Bearer"
