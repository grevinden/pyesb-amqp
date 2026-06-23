from typing import Protocol, runtime_checkable


@runtime_checkable
class AmqpMessage(Protocol):
    """Протокол AMQP сообщения (PEP 544).

    Поля соответствуют одноимённым атрибутам rust-класса ``PyAmqpMessage``.

    ``id`` — AMQP ``message-id`` из секции Properties (настоящий идентификатор
    сообщения, обычно UUID от 1С).  Может быть ``None``, если отправитель не
    указал ``message-id``.

    ``delivery_tag`` — транспортный идентификатор доставки (hex).  При каждом
    переподключении счётчик сбрасывается, поэтому для идентификации сообщений
    используйте ``id``, а не ``delivery_tag``.
    """

    id: str | None
    delivery_tag: str
    body: bytes
    properties: dict[str, str]
    durable: bool
    priority: int


@runtime_checkable
class AmqpMessageHandler(Protocol):
    """PEP 544 — асинхронный обработчик AMQP сообщений.

    Первый аргумент — название канала (target address, который 1С указала
    при отправке).  Второй — ``AmqpMessage``.  Возвращает ``True`` (accept)
    или ``False`` (reject).

    Пример::

        async def handler(channel: str, msg: AmqpMessage) -> bool:
            print(f"[{channel}] ID={msg.id}")
            return True
    """

    async def __call__(self, channel: str, msg: AmqpMessage) -> bool: ...
