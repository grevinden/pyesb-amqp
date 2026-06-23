use anyhow::Context;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

use fe2o3_amqp::acceptor::link::{LinkAcceptor, LinkEndpoint};
use fe2o3_amqp::acceptor::session::{ListenerSessionHandle, SessionAcceptor};
use fe2o3_amqp::acceptor::{ConnectionAcceptor, ListenerConnectionHandle, SaslAnonymousMechanism};
use fe2o3_amqp::types::{
    definitions,
    messaging::Body,
    primitives::Value,
};
use fe2o3_amqp::Receiver;

use crate::callback::CallbackTask;
use crate::conversion::delivery_to_data;
use crate::message::PyAmqpMessage;

/// Accept TCP connections and run the AMQP listener loop.
pub(crate) async fn run_server(
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

/// Handle a single AMQP connection.
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
            error!("Session accept error: {e:#}");
            info!("Connection handler done");
        }
    }

    Ok(())
}

/// Handle a single AMQP session — accept links.
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

/// Handle a single receiver link — receive messages and dispatch to callback.
async fn handle_receiver(
    mut receiver: Receiver,
    task_tx: mpsc::Sender<CallbackTask>,
) -> anyhow::Result<()> {
    // recv() returns Err when the connection/session is dropped by the peer.
    // In that case the receiver is already closed — do NOT call .close().
    let mut conn_dropped = true;
    while let Ok(delivery) = receiver.recv::<Body<Value>>().await {
        conn_dropped = false;
        let target_address = receiver
            .target()
            .as_ref()
            .and_then(|t| t.address.as_ref())
            .cloned();

        let msg_data = delivery_to_data(&delivery);
        let py_msg: PyAmqpMessage = msg_data.into();

        // Send to callback thread via channel — tokio worker does NOT block.
        let (result_tx, result_rx) = oneshot::channel();
        let task = CallbackTask {
            target_address,
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
        )
        .await
        {
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
