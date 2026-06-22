# pyesb-amqp

AMQP 1.0 сервер для Python на Rust (fe2o3-amqp + tokio).

Принимает подключения и отдаёт сообщения в callback.  
**Accept** — сообщение обработано. **Reject** — удалённая сторона перешлёт повторно.

---

## Установка

```bash
pip install pyesb-amqp
```

---

## Использование

### Базовый asyncio

```python
import asyncio
from pyesb_amqp import AMQP

async def handle(msg):
    print(f"Получено: {msg.body}")
    return True  # accept

async def main():
    async with AMQP(host="0.0.0.0", port=6698) as server:
        await server.start(handle)
        await asyncio.Event().wait()  # работаем вечно

asyncio.run(main())
```

### FastAPI lifespan

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pyesb_amqp import AMQP

amqp = AMQP()


async def handle(msg):
    print(f"Сообщение: {msg.body}")
    return True  # accept — False = reject (повторная доставка)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await amqp.start(handle)
    yield
    await amqp.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}
```

---

## Callback

```python
async def handler(msg: AmqpMessage) -> bool:
    ...
```

- **`True`** — сообщение принято (`accepted`)
- **`False`** — сообщение отклонено (`rejected`), удалённая сторона перешлёт

Если callback бросил исключение — сервер ловит, логирует и отвечает reject.

---

## AmqpMessage

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `str` | Delivery tag (hex) |
| `body` | `bytes` | Тело сообщения |
| `properties` | `dict[str, str]` | AMQP application properties |
| `durable` | `bool` | Флаг долговечности |
| `priority` | `int` | Приоритет (0–255) |

---

## Разработка

```bash
git clone https://github.com/lifesnap/pyesb-amqp
cd pyesb-amqp
uv venv
source .venv/bin/activate
maturin develop --uv
```

---

## Лицензия

MIT
