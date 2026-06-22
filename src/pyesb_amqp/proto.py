from typing import Protocol, runtime_checkable


@runtime_checkable
class AmqpMessage(Protocol):
    """Протокол AMQP сообщения (PEP 544).

    Поля соответствуют одноимённым атрибутам rust-класса ``AmqpMessage``.
    """

    id: str
    body: bytes
    properties: dict[str, str]
    durable: bool
    priority: int


@runtime_checkable
class AmqpMessageHandler(Protocol):
    """PEP 544 — асинхронный обработчик AMQP сообщений.

    Должен быть async def, принимает ``AmqpMessage``, возвращает ``True`` (accept)
    или ``False`` (reject).
    """

    async def __call__(self, msg: AmqpMessage) -> bool: ...
