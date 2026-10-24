//! The websocket endpoint clients keep open to the service.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use rand::RngExt;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use crate::api::{Caller, authenticate};
use crate::service::{CONN_QUEUE, ConnHandle, Outbound, Service};

/// Pings go out this often; a connection that sends nothing for twice as long is dropped.
const PING_INTERVAL: Duration = Duration::from_secs(10);
/// The largest message accepted.
const MAX_MESSAGE: usize = 1024 * 1024;
const ID_ALPHABET: &[u8; 32] = b"ybndrfg8ejkmcpqxot1uwisza345h769";

/// A 26 character connection id, as the Go service makes them (clients check the length).
pub fn new_conn_id() -> String {
    let mut bytes = [0u8; 26];
    rand::rng().fill(&mut bytes);
    bytes
        .iter()
        .map(|b| ID_ALPHABET[usize::from(b & 31)] as char)
        .collect()
}

pub async fn upgrade(
    State(svc): State<Arc<Service>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let client_id = match authenticate(&svc, &headers).await {
        Ok(Caller::Client(id)) => id,
        Ok(Caller::Admin) => return (StatusCode::UNAUTHORIZED, "unauthorized\n").into_response(),
        Err((code, msg)) => {
            error!(%msg, "websocket authentication failed");
            return (code, format!("{msg}\n")).into_response();
        }
    };
    ws.max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .on_upgrade(move |socket| serve(svc, client_id, socket))
}

async fn serve(svc: Arc<Service>, client_id: String, socket: WebSocket) {
    let conn_id = new_conn_id();
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Outbound>(CONN_QUEUE);

    // The writer: queued messages and the periodic ping.
    let writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.tick().await;
        loop {
            tokio::select! {
                out = rx.recv() => match out {
                    Some(Outbound::Binary(data)) => {
                        if sink.send(Message::Binary(data.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Outbound::Close) | None => {
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }
                },
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    svc.connected(&conn_id, ConnHandle::new(client_id.clone(), tx));

    // The reader: anything received (a pong included) proves the client alive.
    loop {
        match tokio::time::timeout(2 * PING_INTERVAL, stream.next()).await {
            Err(_) => {
                warn!(conn = %conn_id, "no sign of life from the client, closing the connection");
                break;
            }
            Ok(None) | Ok(Some(Err(_))) => break,
            Ok(Some(Ok(Message::Binary(data)))) => svc.on_frame(&conn_id, &client_id, &data),
            Ok(Some(Ok(Message::Text(_)))) => warn!(conn = %conn_id, "unexpected text message"),
            Ok(Some(Ok(Message::Close(_)))) => break,
            Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => {}
        }
    }
    debug!(conn = %conn_id, "connection ended");
    svc.disconnected(&conn_id);
    writer.abort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_ids_are_26_characters_and_differ() {
        let a = new_conn_id();
        assert_eq!(a.len(), 26);
        assert!(a.bytes().all(|b| ID_ALPHABET.contains(&b)));
        assert_ne!(a, new_conn_id());
    }
}
