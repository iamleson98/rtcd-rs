//! A real browser in the lesson: this serves the page a headless Chrome loads (`page.html`) and
//! relays its signaling to rtcd's websocket the way the LMS server relays lms-web's, so Chrome
//! joins, answers the SFU's offers and watches the teacher exactly as the web app does. The page
//! reports what Chrome measures; those lines are written next to the synthetic receivers'.
//!
//! Page and relay are on loopback: Chrome runs in the same network namespace as this process
//! (`--network container:...`), so whatever shapes that namespace shapes Chrome's media too.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::response::Html;
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use rtcd_proto::{ClientMessage, MessageType, SessionRef};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::class::{message, open_session};
use crate::host::Rtcd;
use crate::stats::Probe;

const PAGE: &str = include_str!("page.html");

/// Serves the page on `port` and the relay on `port + 1`, for as long as the process lives.
pub async fn serve(rtcd: Arc<Rtcd>, call: String, probe: Probe, port: u16) -> std::io::Result<()> {
    let http = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).await?;
    let relay = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port + 1))).await?;
    tokio::spawn(async move {
        let app = Router::new().route("/", get(|| async { Html(PAGE) }));
        let _ = axum::serve(http, app).await;
    });
    tokio::spawn(async move {
        while let Ok((stream, _)) = relay.accept().await {
            tokio::spawn(session(rtcd.clone(), call.clone(), probe.clone(), stream));
        }
    });
    Ok(())
}

/// One page: joins the call for it and carries messages both ways until either side is gone.
async fn session(rtcd: Arc<Rtcd>, call: String, probe: Probe, stream: TcpStream) {
    let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };
    let (mut page_tx, mut page_rx) = socket.split();
    let name = format!("chrome-{}", probe.group);
    let (mut conn, sid, _) = open_session(&rtcd, &call, &name).await;
    loop {
        tokio::select! {
            m = page_rx.next() => {
                let Some(Ok(WsMessage::Text(text))) = m else { break };
                let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                let text_of = |k: &str| v[k].as_str().unwrap_or_default().as_bytes().to_vec();
                match v["t"].as_str() {
                    Some("sdp") => conn.send(message(&sid, &call, MessageType::Sdp, text_of("data"))),
                    Some("ice") => conn.send(message(&sid, &call, MessageType::Ice, text_of("data"))),
                    Some("subscribe") => conn.send(message(
                        &sid,
                        &call,
                        MessageType::VideoSubscribe,
                        json!({ "session_ids": v["data"] }).to_string().into_bytes(),
                    )),
                    Some("recv") => {
                        if let Some(id) = v["id"].as_str() {
                            probe.recv(id, v["fields"].clone());
                        }
                    }
                    Some("event") => probe.event(
                        &name,
                        v["what"].as_str().unwrap_or("error"),
                        v["detail"].as_str().unwrap_or_default(),
                    ),
                    _ => {}
                }
            }
            m = conn.rx.recv() => match m {
                Some(ClientMessage::Rtc(m)) if m.session_id == sid => {
                    let t = match m.kind {
                        MessageType::Sdp => "sdp",
                        MessageType::Ice => "ice",
                        _ => continue,
                    };
                    let data = String::from_utf8_lossy(&m.data).into_owned();
                    if page_tx.send(WsMessage::text(json!({ "t": t, "data": data }).to_string())).await.is_err() {
                        break;
                    }
                }
                Some(ClientMessage::Close(r)) if r.session_id == sid => {
                    probe.event(&name, "error", "the SFU closed the session");
                    break;
                }
                Some(_) => {}
                None => {
                    probe.event(&name, "error", "the signaling connection closed");
                    break;
                }
            },
        }
    }
    conn.send(ClientMessage::Leave(SessionRef { session_id: sid }));
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}
