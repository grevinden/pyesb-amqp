import re

from fastapi import APIRouter, FastAPI

from .models import ChannelDesription, ChannelMetadata, ChannelRuntime, Token


def add_routes[T: APIRouter | FastAPI](*descr: ChannelDesription, app: T) -> T:

    @app.post("/auth/oidc/token")
    async def token_endpoint() -> Token:
        return Token()

    @app.get("/sys/esb/metadata/channels")
    async def get_metadata() -> list[ChannelMetadata]:
        nonlocal descr
        return [ChannelMetadata.model_validate(v) for v in descr]

    @app.get("/sys/esb/runtime/channels")
    async def get_runtime() -> ChannelRuntime:
        nonlocal descr
        return ChannelRuntime(
            items=[ChannelRuntime.Metadata.model_validate(v) for v in descr],
            port=6698,
        )

    return app
