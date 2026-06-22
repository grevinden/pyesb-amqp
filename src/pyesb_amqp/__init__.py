"""pyesb_amqp — AMQP 1.0 server for Python.

High-performance AMQP 1.0 message broker built on Rust (fe2o3-amqp + tokio).
Принимает сообщения от кого угодно, никакой авторизации.
"""

from __future__ import annotations

from .core import AmqpServer
from .proto import AmqpMessage, AmqpMessageHandler

__all__ = [
    "AmqpMessage",
    "AmqpMessageHandler",
    "AmqpServer",
]
