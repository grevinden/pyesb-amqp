from fastapi import APIRouter, FastAPI

from .models import ChannelMetadata, ChannelRuntime, ChannelsDesription, Token


def add_routes(*descr: ChannelsDesription, app: APIRouter | FastAPI):

    @app.post("/auth/oidc/token")
    async def token_endpoint() -> Token:
        return Token()

    @app.get("/sys/esb/metadata/channels")
    async def get_metadata() -> list[ChannelMetadata]:
        nonlocal descr
        return [ChannelMetadata.model_validate(v) for v in descr]

    @app.get("/sys/esb/runtime/channels")
    async def get_runtime() -> ChannelRuntime:
        return ChannelRuntime(
            items=[ChannelRuntime.Metadata.model_validate(v) for v in descr],
            port=6698,
        )
