from fastapi import APIRouter

from .models import ChannelMetadata, ChannelRuntime, Token

router = APIRouter()


@router.post("/auth/oidc/token")
async def token_endpoint() -> Token:
    return Token()


@router.get("/sys/esb/metadata/channels")
async def get_metadata() -> list[ChannelMetadata]:
    return [
        ChannelMetadata(
            process="pyesb",
            process_description="DeadSnake.app",
            channel="outgoing",
            channel_description="FlyAway",
            access="WRITE_ONLY",
        )
    ]


@router.get("/sys/esb/runtime/channels")
async def get_runtime() -> ChannelRuntime:
    return ChannelRuntime(
        items=[
            ChannelRuntime.Metadata(
                process="pyesb", channel="outgoing", destination="queue"
            ),
        ],
        port=6698,
    )
