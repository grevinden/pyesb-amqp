use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::Context;
use pyo3::exceptions::{PyException, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};
use pyo3::types::PyAnyMethods;
use pyo3::Python;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use fe2o3_amqp::acceptor::{
    link::{LinkAcceptor, LinkEndpoint},
    session::{ListenerSessionHandle, SessionAcceptor},
    ConnectionAcceptor, ListenerConnectionHandle, SaslAnonymousMechanism,
};
use fe2o3_amqp::types::{
    definitions,
    messaging::{ApplicationProperties, Body, MessageId, Properties},
    primitives::{SimpleValue, Value},
};
use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp::{Delivery, Receiver};

/// Максимум сообщений в очереди к Python-хендлеру.
/// Когда буфер полон, Tokio-воркеры await на send() → AMQP flow control
/// тормозит отправителя.  Bounded канал предотвращает бесконечное накопление.
const CALLBACK_CHANNEL_CAP: usize = 1_000;

// ---------------------------------------------------------------------------
// Python-facing message type
// ---------------------------------------------------------------------------

/// AmqpMessage — 1:1 с полями Message<Body<Value>> из fe2o3-amqp.
/// Сложные типы разобраны в плоские HashMap<String, serde_json::Value>.
#[pyclass(name = "AmqpMessage", module = "pyesb_amqp", skip_from_py_object)]
#[derive(Clone, Debug)]
pub struct PyAmqpMessage {
    // --- Delivery ---
    #[pyo3(get)]
    pub delivery_id: i64,
    pub delivery_tag: Vec<u8>,
    #[pyo3(get)]
    pub message_format: Option<i64>,
    #[pyo3(get)]
    pub rcv_settle_mode: Option<String>,
    #[pyo3(get)]
    pub link_output_handle: u32,

    // --- Message ---
    pub header: Option<HashMap<String, serde_json::Value>>,
    pub delivery_annotations: Option<HashMap<String, serde_json::Value>>,
    pub message_annotations: Option<HashMap<String, serde_json::Value>>,
    pub properties: Option<Properties>,
    pub application_properties: Option<HashMap<String, String>>,
    pub body: Vec<u8>,
    pub footer: Option<HashMap<String, serde_json::Value>>,
}


// Helper methods — not exposed to Python
impl PyAmqpMessage {
    fn json_map<'py>(&self, py: Python<'py>, map: &Option<HashMap<String, serde_json::Value>>) -> Option<Py<PyAny>> {
        map.as_ref().map(|m| {
            let s = serde_json::to_string(m).unwrap_or_default();
            py.import("json")
                .and_then(|mod_| mod_.call_method1("loads", (s,)))
                .map(|b| b.into())
                .unwrap_or_else(|_| py.None())
        })
    }

    fn str_map<'py>(&self, py: Python<'py>, map: &Option<HashMap<String, String>>) -> Option<Py<PyAny>> {
        map.as_ref().map(|m| {
            let s = serde_json::to_string(m).unwrap_or_default();
            py.import("json")
                .and_then(|mod_| mod_.call_method1("loads", (s,)))
                .map(|b| b.into())
                .unwrap_or_else(|_| py.None())
        })
    }
}

#[pymethods]
impl PyAmqpMessage {
    #[getter]
    fn delivery_tag<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        PyBytes::new(py, &self.delivery_tag).into()
    }

    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        PyBytes::new(py, &self.body).into()
    }

    #[getter]
    fn header<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.json_map(py, &self.header)
    }

    #[getter]
    fn delivery_annotations<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.json_map(py, &self.delivery_annotations)
    }

    #[getter]
    fn message_annotations<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.json_map(py, &self.message_annotations)
    }

    #[getter]
    fn properties<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.properties.as_ref().map(|props| {
            let d = pyo3::types::PyDict::new(py);

            // message_id: varies — int/bytes/str
            if let Some(ref v) = props.message_id {
                match v {
                    MessageId::Ulong(n) => d.set_item("message_id", *n).ok(),
                    MessageId::Uuid(uuid) => {
                        d.set_item("message_id", PyBytes::new(py, uuid.as_inner())).ok()
                    }
                    MessageId::Binary(b) => {
                        d.set_item("message_id", PyBytes::new(py, b.as_ref())).ok()
                    }
                    MessageId::String(s) => d.set_item("message_id", s).ok(),
                };
            }

            // user_id: bytes
            if let Some(ref v) = props.user_id {
                d.set_item("user_id", PyBytes::new(py, v.as_ref())).ok();
            }

            // to / reply_to: strings (Address = String)
            if let Some(ref v) = props.to {
                d.set_item("to", v).ok();
            }
            if let Some(ref v) = props.subject {
                d.set_item("subject", v).ok();
            }
            if let Some(ref v) = props.reply_to {
                d.set_item("reply_to", v).ok();
            }

            // correlation_id: same as message_id
            if let Some(ref v) = props.correlation_id {
                match v {
                    MessageId::Ulong(n) => d.set_item("correlation_id", *n).ok(),
                    MessageId::Uuid(uuid) => {
                        d.set_item("correlation_id", PyBytes::new(py, uuid.as_inner())).ok()
                    }
                    MessageId::Binary(b) => {
                        d.set_item("correlation_id", PyBytes::new(py, b.as_ref())).ok()
                    }
                    MessageId::String(s) => d.set_item("correlation_id", s).ok(),
                };
            }

            // content_type / content_encoding: Symbol → str
            if let Some(ref v) = props.content_type {
                d.set_item("content_type", v.to_string()).ok();
            }
            if let Some(ref v) = props.content_encoding {
                d.set_item("content_encoding", v.to_string()).ok();
            }

            // timestamps: ms since epoch as int
            if let Some(ref v) = props.absolute_expiry_time {
                d.set_item("absolute_expiry_time", v.milliseconds()).ok();
            }
            if let Some(ref v) = props.creation_time {
                d.set_item("creation_time", v.milliseconds()).ok();
            }

            // group_id / reply_to_group_id: strings
            if let Some(ref v) = props.group_id {
                d.set_item("group_id", v).ok();
            }
            if let Some(ref v) = props.group_sequence {
                d.set_item("group_sequence", *v).ok();
            }
            if let Some(ref v) = props.reply_to_group_id {
                d.set_item("reply_to_group_id", v).ok();
            }

            d.into()
        })
    }

    #[getter]
    fn application_properties<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.str_map(py, &self.application_properties)
    }

    #[getter]
    fn footer<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.json_map(py, &self.footer)
    }

    fn __repr__(&self) -> String {
        format!(
            "AmqpMessage(delivery_id={}, delivery_tag={}, body_len={})",
            self.delivery_id,
            hex::encode(&self.delivery_tag),
            self.body.len(),
        )
    }
}

// ---------------------------------------------------------------------------
// Callback thread — executes Python callbacks on a dedicated thread so that
// no tokio worker thread is ever blocked.
// ---------------------------------------------------------------------------

/// A pending callback task: message data + channel to send the result back.
struct CallbackTask {
    py_msg: PyAmqpMessage,
    result_tx: oneshot::Sender<bool>,
}

/// Dedicated background thread that runs Python callbacks.
///
/// The thread acquires the GIL, converts the Rust-side message to a Python
/// ``AmqpMessage``, invokes the user callback, and sends the boolean result
/// back through a oneshot channel.  Tokio workers never block — they just
/// ``.await`` the result.
struct CallbackProcessor {
    task_tx: Option<mpsc::Sender<CallbackTask>>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl CallbackProcessor {
    fn new(callback: SharedCallback, loop_ref: Py<PyAny>) -> Self {
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
                                let outcome = Python::try_attach(
                                    |py| -> PyResult<bool> {
                                        call_python_callback(
                                            py, cb, task.py_msg, &loop_ref,
                                        )
                                    },
                                );
                                match outcome {
                                    None => {
                                        error!(
                                            "Failed to acquire GIL — rejecting"
                                        );
                                        false
                                    }
                                    Some(Err(e)) => {
                                        error!(
                                            "Python callback raised: {e}"
                                        );
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

    fn task_sender(&self) -> Option<mpsc::Sender<CallbackTask>> {
        self.task_tx.clone()
    }

    fn shutdown(&mut self) {
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
fn call_python_callback(
    py: Python<'_>,
    cb: &Py<PyAny>,
    py_msg: PyAmqpMessage,
    loop_ref: &Py<PyAny>,
) -> PyResult<bool> {
    let obj = Py::new(py, py_msg)?;
    let result: Py<PyAny> = cb.call1(py, (obj,))?;
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
    let future = asyncio.call_method1(
        "run_coroutine_threadsafe",
        (result_bound, loop_bound),
    )?;
    let result_obj = future.call_method0("result")?;
    result_obj.extract::<bool>()
}

// ---------------------------------------------------------------------------
// Shared callback — Arc<Mutex<Option<Py<PyAny>>>>
// ---------------------------------------------------------------------------

/// Thread-safe holder for the Python callback.
///
/// - `Py<PyAny>` is `Send` but not `Sync`, so we wrap it in `Mutex`.
/// - `Arc` lets us share it across tokio tasks.
/// - The inner `Option` allows the callback to be set, replaced, or cleared
///   at any time (even while the server is running).
type SharedCallback = Arc<Mutex<Option<Py<PyAny>>>>;

fn make_shared_callback() -> SharedCallback {
    Arc::new(Mutex::new(None))
}

// ---------------------------------------------------------------------------
// Python-facing Server class
// ---------------------------------------------------------------------------

/// Async AMQP 1.0 server built on fe2o3-amqp + tokio.
///
/// Runs a tokio runtime in a background thread and calls the registered
/// Python callback for each received message.
///
/// The callback **must** return ``True`` to accept the message or ``False``
/// to reject it (the remote peer will redeliver a rejected message).
///
/// Usage (прямой доступ к Rust-классу)::
///
///     from pyesb_amqp.amqp import Server
///
///     async def handler(msg):
///         print(f"Got: {msg.body}")
///         return True          # accept
///
///     server = Server(host="0.0.0.0", port=6698)
///     server.on_message(handler)
///     server.start()
///     ...
///     server.stop()
#[pyclass(name = "Server", module = "pyesb_amqp")]
pub struct PyServer {
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
    /// The callback receives an ``AmqpMessage`` and must return ``True`` to
    /// accept the message or ``False`` to reject it (the remote peer will
    /// then redeliver the message later).
    ///
    /// May be called before **or** after ``start()``, and may be called
    /// multiple times to replace an existing callback.
    fn on_message(&mut self, callback: Py<PyAny>) {
        match self.callback {
            Some(ref cb) => {
                *cb.lock().unwrap() = Some(callback);
            }
            None => {
                self.callback = Some(Arc::new(Mutex::new(Some(callback))));
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
                info!("AMQP server ready on {host_for_log}:{port}, container_id={cid_for_log}");
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

// ---------------------------------------------------------------------------
// Core AMQP server  (runs inside the tokio runtime)
// ---------------------------------------------------------------------------



async fn run_server(
    host: &str,
    port: u16,
    container_id: &str,
    task_tx: mpsc::Sender<CallbackTask>,
    mut shutdown_rx: oneshot::Receiver<()>,
    ready_tx: oneshot::Sender<()>,
) -> anyhow::Result<()> {
    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind to {addr}"))?;
    info!("Bound to {addr}");

    // Сигналим Python-стороне, что listener готов.
    let _ = ready_tx.send(());

    let connection_acceptor = ConnectionAcceptor::builder()
        .container_id(container_id)
        .sasl_acceptor(SaslAnonymousMechanism {})
        .build();

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown_rx => {
                info!("Shutdown signal received, stopping server");
                break;
            }

            accept_result = listener.accept() => {
                let (stream, peer_addr) = match accept_result {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Accept error (continuing): {e}");
                        continue;
                    }
                };
                info!("Incoming connection from {peer_addr}");
                let conn = match connection_acceptor.accept(stream).await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("AMQP connection handshake failed from {peer_addr}: {e}");
                        continue;
                    }
                };
                let tx = task_tx.clone();

                tokio::spawn(async move {
                    if let Err(e) = handle_connection(conn, tx).await {
                        error!("Connection handler for {peer_addr} error: {e}");
                    }
                });
            }
        }
    }

    Ok(())
}

async fn handle_connection(
    mut connection: ListenerConnectionHandle,
    task_tx: mpsc::Sender<CallbackTask>,
) -> anyhow::Result<()> {
    info!("Handling new connection");
    let session_acceptor = SessionAcceptor::default();

    match session_acceptor.accept(&mut connection).await {
        Ok(session) => {
            info!("Session accepted, spawning handler");
            let tx = task_tx.clone();
            tokio::spawn(async move {
                // Keep the connection handle alive for the session's lifetime.
                // Dropping ListenerConnectionHandle sends Close to the connection
                // engine, which closes outgoing_session_frames — the session
                // engine won't be able to send frames after that.
                let _conn = connection;
                if let Err(e) = handle_session(session, tx).await {
                    error!("Session handler error: {e}");
                }
                // connection dropped here → Close sent to connection engine
                info!("Connection handler done (session finished)");
            });
        }
        Err(e) => {
            error!("Session accept error: {e:?}");
            info!("Connection handler done");
        }
    }

    Ok(())
}

async fn handle_session(
    mut session: ListenerSessionHandle,
    task_tx: mpsc::Sender<CallbackTask>,
) -> anyhow::Result<()> {
    let link_acceptor = LinkAcceptor::builder()
        .verify_incoming_target(false)
        .build();

    let mut has_links = false;
    while let Ok(link) = link_acceptor.accept(&mut session).await {
        has_links = true;
        match link {
            LinkEndpoint::Sender(_sender) => {
                warn!("Sender link from remote peer is not supported — dropping");
            }
            LinkEndpoint::Receiver(receiver) => {
                let tx = task_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_receiver(receiver, tx).await {
                        error!("Receiver handler error: {e}");
                    }
                });
            }
        }
    }

    // Если линки были — пробуем штатно закрыть сессию.  При разрыве соединения
    // 1С on_end() упадёт — логируем на info как ожидаемое поведение.
    if has_links {
        match session.on_end().await {
            Ok(_) => {}
            Err(e) => info!("Session already ended (1C disconnected): {e}"),
        }
    }
    Ok(())
}

async fn handle_receiver(
    mut receiver: Receiver,
    task_tx: mpsc::Sender<CallbackTask>,
) -> anyhow::Result<()> {
    // recv() returns Err when the connection/session is dropped by the peer.
    // In that case the receiver is already closed — do NOT call .close().
    let mut conn_dropped = true;
    while let Ok(delivery) = receiver.recv::<Body<Value>>().await {
        conn_dropped = false;
        let msg_data = delivery_to_data(&delivery);
        let py_msg = PyAmqpMessage {
            delivery_id: msg_data.delivery_id,
            delivery_tag: msg_data.delivery_tag,
            message_format: msg_data.message_format,
            rcv_settle_mode: msg_data.rcv_settle_mode,
            link_output_handle: msg_data.link_output_handle,
            header: msg_data.header,
            delivery_annotations: msg_data.delivery_annotations,
            message_annotations: msg_data.message_annotations,
            properties: msg_data.properties,
            application_properties: Some(msg_data.application_properties),
            body: msg_data.body,
            footer: msg_data.footer,
        };
        // msg_data dropped here; fields moved into py_msg

        // Send to callback thread via channel — tokio worker does NOT block.
        let (result_tx, result_rx) = oneshot::channel();
        let task = CallbackTask {
            py_msg,
            result_tx,
        };

        // Backpressure: если очередь полна — send() ждёт, пока CallbackProcessor
        // освободит слот.  AMQP receive loop приостанавливается → flow control
        // автоматически тормозит отправителя.
        if task_tx.send(task).await.is_err() {
            error!("Callback thread unavailable — rejecting message");
            receiver
                .reject(&delivery, None::<definitions::Error>)
                .await?;
            continue;
        }

        // Non-blocking wait for the callback result with timeout.
        // Страховка: зависший Python-хендлер не блокирует канал навсегда.
        let accepted = match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            result_rx,
        ).await {
            Ok(Ok(val)) => val,
            Ok(Err(_)) => {
                error!("Callback thread dropped without responding — rejecting");
                false
            }
            Err(_) => {
                error!("Python handler timed out after 30s — rejecting message");
                false
            }
        };

        if accepted {
            receiver.accept(&delivery).await?;
        } else {
            receiver
                .reject(&delivery, None::<definitions::Error>)
                .await?;
        }
    }

    // Если хотя бы одно сообщение было получено — пробуем штатно закрыть
    // receiver.  При разрыве соединения 1С close() упадёт — логируем на info
    // как ожидаемое поведение.
    if !conn_dropped {
        match receiver.close().await {
            Ok(_) => {}
            Err(e) => info!("Receiver already closed (1C disconnected): {e}"),
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Message conversion
// ---------------------------------------------------------------------------

fn header_to_json(hdr: &fe2o3_amqp::types::messaging::Header) -> HashMap<String, serde_json::Value> {
    let mut m = HashMap::new();
    m.insert("durable".into(), serde_json::Value::Bool(hdr.durable));
    m.insert("priority".into(), serde_json::json!(hdr.priority.0));
    if let Some(ttl) = &hdr.ttl {
        m.insert("ttl".into(), serde_json::json!(ttl));
    }
    m.insert("first_acquirer".into(), serde_json::Value::Bool(hdr.first_acquirer));
    m.insert("delivery_count".into(), serde_json::json!(hdr.delivery_count));
    m
}


fn delivery_to_data(delivery: &Delivery<Body<Value>>) -> MessageData {
    let message = delivery.message();

    // -- Delivery fields --------------------------------------------
    // DeliveryNumber = u32, get by value via deref
    let delivery_id = *delivery.delivery_id() as i64;
    let delivery_tag = delivery.delivery_tag().as_ref().to_vec();
    // message_format() returns &Option<MessageFormat>
    // MessageFormat = u32 (type alias), no .0 needed
    let message_format = delivery.message_format().map(|mf| mf as i64);
    // rcv_settle_mode is pub(crate) — get via DeliveryInfo
    let info = DeliveryInfo::from(delivery);
    let rcv_settle_mode = info
        .rcv_settle_mode()
        .as_ref()
        .map(|m| format!("{:?}", m));
    // Handle is newtype over u32
    let link_output_handle = delivery.handle().0;

    // -- body -------------------------------------------------------
    let body = body_to_bytes(delivery.body());

    // -- header -----------------------------------------------------
    let header = message.header.as_ref().map(header_to_json);

    // -- annotations (skip complex type conversion) -----------------
    // DeliveryAnnotations, MessageAnnotations, Footer are different types
    // from Annotations — just serialize through serde_json::to_value
    let delivery_annotations = message
        .delivery_annotations
        .as_ref()
        .and_then(|ann| serde_json::to_value(ann).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(obj) if !obj.is_empty() => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k, v);
                }
                Some(map)
            }
            _ => None,
        });

    let message_annotations = message
        .message_annotations
        .as_ref()
        .and_then(|ann| serde_json::to_value(ann).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(obj) if !obj.is_empty() => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k, v);
                }
                Some(map)
            }
            _ => None,
        });

    let footer = message
        .footer
        .as_ref()
        .and_then(|ann| serde_json::to_value(ann).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(obj) if !obj.is_empty() => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k, v);
                }
                Some(map)
            }
            _ => None,
        });

    // -- properties -------------------------------------------------
    let properties = message.properties.clone();

    // -- application_properties -------------------------------------
    let application_properties = message
        .application_properties
        .as_ref()
        .map(|ap| extract_properties(Some(ap)));

    MessageData {
        delivery_id,
        delivery_tag,
        message_format,
        rcv_settle_mode,
        link_output_handle,
        header,
        delivery_annotations,
        message_annotations,
        properties,
        application_properties: application_properties.unwrap_or_default(),
        body,
        footer,
    }
}

/// Plain data struct used before conversion to the Python class.
struct MessageData {
    // Delivery
    delivery_id: i64,
    delivery_tag: Vec<u8>,
    message_format: Option<i64>,
    rcv_settle_mode: Option<String>,
    link_output_handle: u32,

    // Message
    header: Option<HashMap<String, serde_json::Value>>,
    delivery_annotations: Option<HashMap<String, serde_json::Value>>,
    message_annotations: Option<HashMap<String, serde_json::Value>>,
    properties: Option<Properties>,
    application_properties: HashMap<String, String>,
    body: Vec<u8>,
    footer: Option<HashMap<String, serde_json::Value>>,
}

fn body_to_bytes(body: &Body<Value>) -> Vec<u8> {
    match body {
        Body::Data(batch) => batch
            .iter()
            .flat_map(|data| data.0.as_ref().to_vec())
            .collect(),
        Body::Value(amqp_val) => match &amqp_val.0 {
            Value::Binary(b) => b.as_ref().to_vec(),
            Value::String(s) => s.as_bytes().to_vec(),
            other => serde_json::to_vec(other).unwrap_or_default(),
        },
        Body::Sequence(batch) => serde_json::to_vec(batch).unwrap_or_default(),
        Body::Empty => vec![],
    }
}

fn extract_properties(app_props: Option<&ApplicationProperties>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(props) = app_props {
        for (key, val) in props.iter() {
            let s = simple_value_to_string(val);
            map.insert(key.clone(), s);
        }
    }
    map
}


fn simple_value_to_string(val: &SimpleValue) -> String {
    match val {
        SimpleValue::String(s) => s.clone(),
        SimpleValue::Symbol(s) => s.to_string(),
        SimpleValue::Binary(b) => String::from_utf8_lossy(b.as_ref()).to_string(),
        SimpleValue::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_else(|_| format!("{other:?}")),
    }
}

// ---------------------------------------------------------------------------
// PyO3 module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn amqp(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyServer>()?;
    m.add_class::<PyAmqpMessage>()?;
    Ok(())
}
