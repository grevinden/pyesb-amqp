//! pyesb_amqp — AMQP 1.0 server for Python built on fe2o3-amqp + tokio.
//!
//! Модули организованы по зонам ответственности:
//!
//! - [`message`] — Python-класс ``AmqpMessage`` (pyclass getters/setters)
//! - [`conversion`] — конвертация AMQP-типов в плоские Rust-структуры
//! - [`callback`] — тред для вызова Python-колбэков (CallbackProcessor)
//! - [`runtime`] — tokio-сеть: приём соединений, сессий, receiver-линков
//! - [`server`] — основной ``Server`` pyclass (публичный API для Python)

pub(crate) mod message;
pub(crate) mod conversion;
pub(crate) mod callback;
pub(crate) mod runtime;
pub(crate) mod server;

use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// PyO3 module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn amqp(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<server::PyServer>()?;
    m.add_class::<message::PyAmqpMessage>()?;
    Ok(())
}
