from __future__ import annotations

from typing import Protocol, TypedDict, runtime_checkable

# ---------------------------------------------------------------------------
# AMQP Header
# ---------------------------------------------------------------------------


class Header(TypedDict, total=False):
    """AMQP header section (``@pyo3(getter) fn header``).

    Все ключи опциональны — отправитель может опустить любое поле.
    """

    durable: bool
    priority: int
    ttl: int
    first_acquirer: bool
    delivery_count: int


# ---------------------------------------------------------------------------
# AMQP Properties
# ---------------------------------------------------------------------------


class Properties(TypedDict, total=False):
    """AMQP properties section (``@pyo3(getter) fn properties``).

    Все ключи опциональны — заполняется только то, что указал отправитель.
    Сам словарь может быть ``None``, если секция properties отсутствует.
    """

    message_id: int | bytes | str
    user_id: bytes
    to: str
    subject: str
    reply_to: str
    correlation_id: int | bytes | str
    content_type: str
    content_encoding: str
    absolute_expiry_time: int
    creation_time: int
    group_id: str
    group_sequence: int
    reply_to_group_id: str


# ---------------------------------------------------------------------------
# AMQP Delivery / Message Annotations
# ---------------------------------------------------------------------------


class DeliveryAnnotations(TypedDict, total=False):
    """Delivery annotations — 1С не использует, всегда ``None``."""

    pass


class MessageAnnotations(TypedDict, total=False):
    """Message annotations — 1С не использует, всегда ``None``."""

    pass


# ---------------------------------------------------------------------------
# AMQP Application Properties
# ---------------------------------------------------------------------------


class ApplicationProperties(TypedDict, total=False):
    """Application properties — прикладные пары ключ-значение от 1С.

    Все значения — строки (Rust присылает ``HashMap<String, String>``).
    Известные ключи (интеграционные префиксы ``integ_*``):

    * ``integ_message_id`` — UUID сообщения (обязателен)
    * ``integ_message_body_size`` — размер тела (строка, число)
    * ``integ_sender_code`` — код отправителя
    * ``integ_recipient_code`` — код получателя (может быть CSV)
    * ``integ_message_correlation_id`` — UUID корреляции
    """

    integ_sender_code: str
    integ_recipient_code: str
    integ_message_body_size: str
    integ_message_correlation_id: str
    integ_message_id: str


# ---------------------------------------------------------------------------
# AMQP Footer
# ---------------------------------------------------------------------------


class Footer(TypedDict, total=False):
    """Footer — 1С не использует, всегда ``None``."""

    pass


# ---------------------------------------------------------------------------
# AmqpMessage — основной протокол
# ---------------------------------------------------------------------------


@runtime_checkable
class AmqpMessage(Protocol):
    """Протокол AMQP сообщения (PEP 544).

    Поля соответствуют одноимённым атрибутам rust-класса ``PyAmqpMessage``.

    Секции сообщения:

    * **delivery** — транспортная информация о доставке
    * **header** — заголовок AMQP (durable, priority, ttl и др.)
    * **annotations** — произвольные метаданные
    * **properties** — стандартные AMQP-свойства
    * **application_properties** — произвольные пары ключ-значение
    * **body** — тело сообщения
    * **footer** — служебный хвост сообщения
    """

    # -- delivery ---------------------------------------------------------

    delivery_id: int
    """Уникальный идентификатор доставки в рамках сессии."""

    delivery_tag: bytes
    """Транспортный тег доставки (raw bytes, не hex).

    При каждом переподключении счётчик сбрасывается — НЕ используйте
    для идентификации сообщений.  Для этого есть ``properties["message_id"]``.
    """

    message_format: int | None
    """Формат сообщения AMQP (если указан отправителем)."""

    rcv_settle_mode: str | None
    """Режим подтверждения приёма (например ``"first"``, ``"second"``)."""

    link_output_handle: int
    """Handle линка, через который пришло сообщение."""

    # -- header -----------------------------------------------------------

    header: Header | None
    """Заголовок AMQP — см. ``Header``."""

    # -- annotations ------------------------------------------------------

    delivery_annotations: DeliveryAnnotations | None
    """Аннотации доставки — см. ``DeliveryAnnotations``."""

    message_annotations: MessageAnnotations | None
    """Аннотации сообщения — см. ``MessageAnnotations``."""

    # -- properties -------------------------------------------------------

    properties: Properties | None
    """Стандартные AMQP-свойства — см. ``Properties``.

    Пример::

        msg.properties["message_id"]   # str | int | bytes | None
        msg.properties["content_type"] # "application/json" | None
    """

    # -- application properties -------------------------------------------

    application_properties: ApplicationProperties | None
    """Прикладные свойства — см. ``ApplicationProperties``."""

    # -- body -------------------------------------------------------------

    body: bytes
    """Тело сообщения (raw bytes)."""

    # -- footer -----------------------------------------------------------

    footer: Footer | None
    """Служебный хвост сообщения — см. ``Footer``."""


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


@runtime_checkable
class AmqpMessageHandler(Protocol):
    """PEP 544 — асинхронный обработчик AMQP сообщений.

    Первый аргумент — название канала (``target_address``, который 1С
    указала в AMQP Attach-фрейме).  Второй — ``AmqpMessage``.

    Возвращает ``True`` (accept — сообщение будет подтверждено) или
    ``False`` (reject — отправитель получит уведомление об отказе и может
    попробовать снова).

    Пример::

        async def handler(destination: str, msg: AmqpMessage) -> bool:
            print(f"[{destination}] ID={msg.properties['message_id']}")
            return True
    """

    async def __call__(self, destination: str, msg: AmqpMessage, /) -> bool: ...
