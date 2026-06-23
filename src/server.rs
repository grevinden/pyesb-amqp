use std::sync::Arc;
use std::thread;

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use tokio::sync::oneshot;
use tracing::{error, info};

use crate::callback::{make_shared_callback, CallbackProcessor, SharedCallback};
use crate::runtime::run_server;

/// Async AMQP 1.0 server built on fe2o3-amqp + tokio.
///
/// Runs a tokio runtime in a background thread and calls the registered
/// Python callback for each received message.
///
/// The callback **must** return ``True`` to accept the message or ``False``
/// to reject it (the remote peer will redeliver a rejected message).
///
/// Usage::
///
///     from pyesb_amqp.amqp import Server
///
///     async def handler(channel: str, msg: AmqpMessage) -> bool:
///         print(f"[{channel}] Got: {msg.body}")
///         return True          # accept
///
///     server = Server(host="0.0.0.0", port=6698)
///     server.on_message(handler)
///     server.start()
///     ...
///     server.stop()
#[pyclass(name = "Server", module = "pyesb_amqp")]
pub(crate) struct PyServer {
    host: String,
    port: u16,
    container_id: String,
    callback: Option<SharedCallback>,
    callback_processor: Option<CallbackProcessor>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    thread_handle: Option<thread::JoinHandle<()>>,
    loop_ref: Option<Py<PyAny>>,
}

#[pymethods]
impl PyServer {
    #[new]
    #[pyo3(signature = (
        host = "0.0.0.0".to_string(),
        port = 6698,
        container_id = "pyesb-broker".to_string(),
    ))]
    fn new(host: String, port: u16, container_id: String) -> Self {
        Self {
            host,
            port,
            container_id,
            callback: None,
            callback_processor: None,
            shutdown_tx: None,
            thread_handle: None,
            loop_ref: None,
        }
    }

    /// Register or update the message callback.
    ///
    /// The callback receives a channel name (str) and an ``AmqpMessage`` and
    /// must return ``True`` to accept the message or ``False`` to reject it
    /// (the remote peer will then redeliver the message later).
    ///
    /// May be called before **or** after ``start()``, and may be called
    /// multiple times to replace an existing callback.
    fn on_message(&mut self, callback: Py<PyAny>) {
        match self.callback {
            Some(ref cb) => {
                *cb.lock().unwrap() = Some(callback);
            }
            None => {
                self.callback = Some(Arc::new(std::sync::Mutex::new(Some(callback))));
            }
        }
    }

    fn set_loop(&mut self, loop_ref: Py<PyAny>) {
        self.loop_ref = Some(loop_ref);
    }

    /// Start the AMQP server in a background thread.
    ///
    /// Blocks until the tokio listener is actually bound (или ошибка).
    ///
    /// Raises ``RuntimeError`` if already running.
    fn start(&mut self) -> PyResult<()> {
        if self.shutdown_tx.is_some() {
            return Err(PyException::new_err("Server is already running"));
        }

        // Initialize tracing once (idempotent).
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .try_init()
            .ok();

        info!(
            "pyesb_amqp v{} starting — host={}, port={}, container_id={}",
            env!("CARGO_PKG_VERSION"),
            self.host,
            self.port,
            self.container_id,
        );

        // If the user never registered a callback, create an empty holder so
        // the server has something to clone.
        let callback = self
            .callback
            .get_or_insert_with(make_shared_callback)
            .clone();

        // Create the callback processor (dedicated thread for Python calls).
        let loop_ref = self.loop_ref.take().ok_or_else(|| {
            PyException::new_err(
                "No event loop reference. Call AMQP.start() to set the loop.",
            )
        })?;

        let processor = CallbackProcessor::new(callback, loop_ref);
        let task_tx = processor
            .task_sender()
            .expect("CallbackProcessor always has a sender after new()");
        self.callback_processor = Some(processor);

        let host = self.host.clone();
        let port = self.port;
        let container_id = self.container_id.clone();

        // Клоны для лога после move.
        let host_for_log = host.clone();
        let cid_for_log = container_id.clone();

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        // Oneshot for synchronisation: tokio thread сигналит, когда listener
        // готов принимать соединения.
        let (ready_tx, ready_rx) = oneshot::channel::<()>();

        let thread_handle = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("amqp-worker")
                .enable_all()
                .build()
                .expect("Failed to build tokio runtime");

            rt.block_on(async move {
                if let Err(e) = run_server(
                    &host, port, &container_id, task_tx, shutdown_rx, ready_tx,
                )
                .await
                {
                    error!("AMQP server error: {e}");
                }
            });
        });

        // Блокируем (GIL удерживается, но tokio-треду GIL не нужен),
        // пока listener не будет готов.
        match ready_rx.blocking_recv() {
            Ok(()) => {
                info!(
                    "AMQP server ready on {host_for_log}:{port}, container_id={cid_for_log}"
                );
                self.shutdown_tx = Some(shutdown_tx);
                self.thread_handle = Some(thread_handle);
                Ok(())
            }
            Err(_) => {
                // Канал закрыт без сигнала → bind не удался.
                // Ждём завершения треда и возвращаем ошибку.
                let _ = thread_handle.join();
                Err(PyException::new_err(
                    "Failed to bind AMQP listener — see stderr for details",
                ))
            }
        }
    }

    /// Stop the server and join the background thread.
    ///
    /// Safe to call even if the server is not running.
    fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Join the tokio thread first. When the runtime is dropped, all
        // spawned tasks are cancelled, dropping their sender clones.
        // The callback thread sees the channel close and exits.
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
        // Now shut down the callback thread.
        self.callback_processor.take();
    }

    // -- context manager support ----------------------------------------

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __exit__(
        mut slf: PyRefMut<'_, Self>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) {
        slf.stop();
    }
}
