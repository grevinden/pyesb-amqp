use std::sync::{Arc, Mutex};
use std::thread;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::message::PyAmqpMessage;

/// Максимум сообщений в очереди к Python-хендлеру.
/// Когда буфер полон, Tokio-воркеры await на send() → AMQP flow control
/// тормозит отправителя.  Bounded канал предотвращает бесконечное накопление.
pub(crate) const CALLBACK_CHANNEL_CAP: usize = 1_000;

// ---------------------------------------------------------------------------
// Shared callback — Arc<Mutex<Option<Py<PyAny>>>>
// ---------------------------------------------------------------------------

/// Thread-safe holder for the Python callback.
///
/// - `Py<PyAny>` is `Send` but not `Sync`, so we wrap it in `Mutex`.
/// - `Arc` lets us share it across tokio tasks.
/// - The inner `Option` allows the callback to be set, replaced, or cleared
///   at any time (even while the server is running).
pub(crate) type SharedCallback = Arc<Mutex<Option<Py<PyAny>>>>;

pub(crate) fn make_shared_callback() -> SharedCallback {
    Arc::new(Mutex::new(None))
}

// ---------------------------------------------------------------------------
// Callback task
// ---------------------------------------------------------------------------

/// A pending callback task: message data + channel to send the result back.
pub(crate) struct CallbackTask {
    pub target_address: Option<String>,
    pub py_msg: PyAmqpMessage,
    pub result_tx: oneshot::Sender<bool>,
}

// ---------------------------------------------------------------------------
// CallbackProcessor — dedicated background thread
// ---------------------------------------------------------------------------

/// Dedicated background thread that runs Python callbacks.
///
/// The thread acquires the GIL, converts the Rust-side message to a Python
/// ``AmqpMessage``, invokes the user callback, and sends the boolean result
/// back through a oneshot channel.  Tokio workers never block — they just
/// ``.await`` the result.
pub(crate) struct CallbackProcessor {
    task_tx: Option<mpsc::Sender<CallbackTask>>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl CallbackProcessor {
    pub fn new(callback: SharedCallback, loop_ref: Py<PyAny>) -> Self {
        let (task_tx, mut task_rx) = mpsc::channel::<CallbackTask>(CALLBACK_CHANNEL_CAP);

        let handle = thread::spawn(move || {
            info!("Callback thread started");
            while let Some(task) = task_rx.blocking_recv() {
                let accepted = {
                    let guard = callback.lock();
                    match guard {
                        Err(e) => {
                            error!("Mutex lock error in callback thread: {e}");
                            false
                        }
                        Ok(ref g) => match g.as_ref() {
                            None => true,
                            Some(ref cb) => {
                                let outcome = Python::try_attach(|py| -> PyResult<bool> {
                                    call_python_callback(
                                        py,
                                        cb,
                                        task.target_address,
                                        task.py_msg,
                                        &loop_ref,
                                    )
                                });
                                match outcome {
                                    None => {
                                        error!("Failed to acquire GIL — rejecting");
                                        false
                                    }
                                    Some(Err(e)) => {
                                        error!("Python callback raised: {e}");
                                        false
                                    }
                                    Some(Ok(ok)) => ok,
                                }
                            }
                        },
                    }
                };
                // Receiver may have been cancelled — ignore error.
                let _ = task.result_tx.send(accepted);
            }
            info!("Callback thread finished");
        });

        Self {
            task_tx: Some(task_tx),
            thread_handle: Some(handle),
        }
    }

    pub fn task_sender(&self) -> Option<mpsc::Sender<CallbackTask>> {
        self.task_tx.clone()
    }

    pub fn shutdown(&mut self) {
        // Drop sender → channel closes → blocking_recv returns None → thread exits.
        self.task_tx.take();
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for CallbackProcessor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Helper: call Python callback handling sync + async
// ---------------------------------------------------------------------------

/// Call the Python callback, supporting both sync (returns ``bool``) and
/// async (returns a coroutine) callbacks.
///
/// For async callbacks, uses ``asyncio.run_coroutine_threadsafe`` to schedule
/// the coroutine on the event loop.  The callback thread **blocks** waiting
/// for the result, but the GIL is released during the wait so the event loop
/// can proceed in the main Python thread.
pub(crate) fn call_python_callback(
    py: Python<'_>,
    cb: &Py<PyAny>,
    target_address: Option<String>,
    py_msg: PyAmqpMessage,
    loop_ref: &Py<PyAny>,
) -> PyResult<bool> {
    let obj = Py::new(py, py_msg)?;
    let channel = target_address.unwrap_or_default();
    let result: Py<PyAny> = cb.call1(py, (channel, obj))?;
    let result_bound = result.bind(py);

    // Sync callback — extract bool directly.
    if let Ok(accepted) = result_bound.extract::<bool>() {
        return Ok(accepted);
    }

    // Check if the result is a coroutine (async def callback).
    let inspect = py.import("inspect")?;
    let is_coro: bool = inspect
        .call_method1("iscoroutine", (result_bound,))?
        .extract()?;

    if !is_coro {
        return Err(PyTypeError::new_err(format!(
            "Callback must return bool or be an async function, got {}",
            result_bound.get_type().name()?,
        )));
    }

    // Async callback: schedule on the event loop and wait for the result.
    // run_coroutine_threadsafe releases the GIL while waiting.
    let asyncio = py.import("asyncio")?;
    let loop_bound = loop_ref.bind(py);
    let future =
        asyncio.call_method1("run_coroutine_threadsafe", (result_bound, loop_bound))?;
    let result_obj = future.call_method0("result")?;
    result_obj.extract::<bool>()
}
