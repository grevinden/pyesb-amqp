from asyncio import get_running_loop
from typing import Self

from pyesb_amqp.proto import AmqpMessageHandler


class AmqpServer:
    """Async AMQP 1.0 server.

    Простейший приёмник сообщений по протоколу AMQP 1.0.
    Работает поверх asyncio, парсинг протокола — в Rust (fe2o3-amqp).

    Usage::

        from pyesb_amqp import AMQP, AmqpMessage

        async def handler(msg: AmqpMessage) -> bool:
            print(f"Got: {msg.body}")
            return True   # accept

        server = AmqpServer(host="0.0.0.0", port=6698)
        await server.start(handler)
        ...
        await server.stop()

    Или через async with::

        from pyesb_amqp import AMQP

        async with AMQP() as server:
            await server.start(handler)
            await asyncio.Event().wait()
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 6698,
        container_id: str = "pyesb-broker",
    ) -> None:
        from .amqp import Server

        self._server = Server(host, port, container_id)
        self._started = False

    async def start(self, handler: AmqpMessageHandler) -> None:
        """Запустить сервер и зарегистрировать обработчик сообщений.

        Args:
            handler: Асинхронная функция, принимающая ``AmqpMessage`` и
                возвращающая ``True`` (accept) или ``False`` (reject).

        Raises:
            RuntimeError: Если нет запущенного asyncio-цикла.
        """
        if self._started:
            return

        # Передаём циклическую ссылку в Rust — для run_coroutine_threadsafe
        self._server.set_loop(get_running_loop())

        # Регистрируем колбэк (sync/async — Rust разберётся сам)
        self._server.on_message(handler)

        # Запускаем tokio runtime в фоновом треде (не блокирует)
        self._server.start()

        self._started = True

    async def stop(self) -> None:
        """Остановить сервер.

        Безопасно вызывать многократно и до ``start()``.
        """
        if not self._started:
            return

        # stop() делает join тредов — выгружаем в executor, чтобы
        # не блокировать asyncio-цикл.
        await get_running_loop().run_in_executor(None, self._server.stop)

        self._started = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.stop()
