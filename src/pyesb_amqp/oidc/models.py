from typing import Literal

from pydantic import Field, PositiveInt
from pydantic.main import BaseModel

"""
OIDC-модели для совместимости с 1С ESB.

    ┌──────────────────────────────────────────────────────┐
    │                     OIDC Models                      │
    ├──────────────────────────────────────────────────────┤
    │                                                      │
    │   ┌──────────────┐                                   │
    │   │  ChannelBase │◄──── inherits ─────────────┐      │
    │   │──────────────│                            │      │
    │   │  process     │  "pyesb"                   │      │
    │   │  channel     │  e.g. "outgoing"           │      │
    │   └──────┬───────┘                            │      │
    │          │                                    │      │
    │    ┌─────┴──────────────────┐    ┌────────────┴──┐   │
    │    │   ChannelMetadata      │    │  Runtime.Meta │   │
    │    │────────────────────────│    │───────────────│   │
    │    │  processDescription    │    │  destination  │   │
    │    │  channelDescription    │    └───────────────┘   │
    │    │  access (R/O | W/O)    │                        │
    │    └────────────────────────┘                        │
    │                                                      │
    │   ┌────────────────┐     ┌────────────────────┐      │
    │   │ ChannelRuntime │────▶│ items: list[Meta]  │      │
    │   │────────────────│     │ port: 6698         │      │
    │   └────────────────┘     └────────────────────┘      │
    │                                                      │
    │   ┌────────────────────────────────────────┐         │
    │   │ Token                                  │         │
    │   │────────────────────────────────────────│         │
    │   │  id_token  = None                      │         │
    │   │  access_token = "Not implemented"      │         │
    │   │  token_type = "Bearer"                 │         │
    │   └────────────────────────────────────────┘         │
    └──────────────────────────────────────────────────────┘
"""


class ChannelBase(BaseModel):
    process: str = Field("pyesb", pattern=f"[a-z0-9]{1 - 15}")
    channel: str = Field(pattern=f"[a-z0-9]{1 - 15}")


class ChannelMetadata(ChannelBase):
    process_description: str = Field(
        "Python ESB", serialization_alias="processDescription", max_length=100
    )
    channel_description: str = Field(
        ..., serialization_alias="channelDescription", max_length=100
    )
    access: Literal["READ_ONLY", "WRITE_ONLY"] = "WRITE_ONLY"


class ChannelRuntime(BaseModel):
    class Metadata(ChannelBase):
        destination: str = Field(pattern=f"[a-z0-9]{1 - 15}")

    items: list[Metadata]
    port: PositiveInt = 6698


class Token(BaseModel):
    id_token: Literal[None] = None
    access_token: Literal["Not implemented"] = "Not implemented"
    token_type: Literal["Bearer"] = "Bearer"
