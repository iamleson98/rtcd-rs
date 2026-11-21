//! A real browser in the lesson: this serves the page a headless Chrome loads (`page.html`) and
//! relays its signaling to rtcd's websocket the way the LMS server relays lms-web's, so Chrome
//! joins, answers the SFU's offers and watches the teacher exactly as the web app does. The page
//! reports what Chrome measures; those lines are written next to the synthetic receivers'.
//!
//! Page and relay are on loopback when Chrome runs in the same network namespace as this process
//! (`--network container:...`), so whatever shapes that namespace shapes Chrome's media too; a Chrome
//! in a namespace of its own (which a network can shape for it alone) reaches them on `--chrome-bind`.

use std::sync::Arc;

use axum::Router;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use rtcd_proto::{ClientMessage, MessageType, SessionRef};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::class::{message, try_open_session};
use crate::host::{Rtcd, RtcdConn};
use crate::stats::{Probe, delay_summary};

const PAGE: &str = include_str!("page.html");
const QUALITY: &str = include_str!("quality.js");

/// What the page gets besides itself.
#[derive(Debug)]
pub struct Page {
    /// The clips the teacher's camera is sent from, as IVF files (`/reference/0.ivf`, `/reference/1.ivf`,
    /// ..., and their number at `/reference/count`): the page decodes them to score the picture it
    /// gets, against the clip it matches best.
    pub references: Vec<Vec<u8>>,
    /// For a browser outside the SFU's network: the address the SFU gives for its media (its own, on
    /// the network the lesson runs on) and the one to put in its place in what the page is sent, the
    /// way an SFU behind NAT offers its public address (`ice_host_override`) to those on the internet.
    pub advertise: Option<(String, String)>,
    /// Recovery as lms-web does it (the page loads with `recovery=1`): the page's websocket stays open
    /// while its session on the SFU does not, see [`session`]. Off, the page's websocket and its
    /// session end together, and the page takes both back.
    pub recovery: bool,
}

/// Serves the page on `bind:port` and the relay on `bind:port + 1`, for as long as the process
/// lives. The browser watches the cameras of the sessions in `watch`, the same ones the synthetic
/// students watch, so that the SFU carries the same load for it.
pub async fn serve(
    rtcd: Arc<Rtcd>,
    call: String,
    probe: Probe,
    watch: Vec<String>,
    page: Page,
    bind: &str,
    port: u16,
) -> std::io::Result<()> {
    let http = TcpListener::bind(format!("{bind}:{port}")).await?;
    let relay = TcpListener::bind(format!("{bind}:{}", port + 1)).await?;
    let references: Arc<Vec<Bytes>> =
        Arc::new(page.references.into_iter().map(Bytes::from).collect());
    let advertise = page.advertise;
    let recovery = page.recovery;
    tokio::spawn(async move {
        let app = Router::new()
            .route("/", get(|| async { Html(PAGE) }))
            .route(
                "/quality.js",
                get(|| async { ([(CONTENT_TYPE, "text/javascript")], QUALITY) }),
            )
            .route(
                "/reference/{n}",
                get(move |Path(n): Path<String>| {
                    let references = references.clone();
                    async move {
                        if n == "count" {
                            return references.len().to_string().into_response();
                        }
                        match n
                            .strip_suffix(".ivf")
                            .and_then(|n| n.parse::<usize>().ok())
                            .and_then(|n| references.get(n))
                        {
                            Some(clip) => {
                                ([(CONTENT_TYPE, "video/x-ivf")], clip.clone()).into_response()
                            }
                            None => StatusCode::NOT_FOUND.into_response(),
                        }
                    }
                }),
            );
        let _ = axum::serve(http, app).await;
    });
    tokio::spawn(async move {
        while let Ok((stream, _)) = relay.accept().await {
            tokio::spawn(session(
                rtcd.clone(),
                call.clone(),
                probe.clone(),
                watch.clone(),
                advertise.clone(),
                recovery,
                stream,
            ));
        }
    });
    Ok(())
}

/// The next message from the SFU's side of the page's session, or never when it has none.
async fn next_from_sfu(live: &mut Option<(RtcdConn, String)>) -> Option<ClientMessage> {
    match live {
        Some((conn, _)) => conn.rx.recv().await,
        None => std::future::pending().await,
    }
}

/// The page's row with the one-way delay of the interval: the samples it computed itself (`ms`) and
/// the playout times of RTP timestamps (`sync`) mapped to the sender's clock, when this process
/// holds it (a page for a group on another machine has none, and its delays are null).
fn with_delay(probe: &Probe, id: &str, v: &Value) -> Value {
    let mut fields = v["fields"].clone();
    let mut delays: Vec<f64> = v["ms"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_f64)
        .collect();
    if let Some((kind, sender)) = id
        .rsplit_once('/')
        .and_then(|(_, stream)| stream.split_once(':'))
    {
        delays.extend(v["sync"].as_array().into_iter().flatten().filter_map(|s| {
            probe.delay_ms(kind, sender, s["rtp"].as_u64()? as u32, s["at"].as_f64()?)
        }));
    }
    let (p50, p95) = delay_summary(&mut delays);
    fields["delay_p50_ms"] = p50.into();
    fields["delay_p95_ms"] = p95.into();
    fields
}

/// One page: carries its messages to the SFU and back until its websocket is gone.
///
/// Without `recovery` the session on the SFU opens with the websocket and ends with it: when the SFU
/// closes the session or is gone, the websocket is closed and the page opens a new one.
///
/// With `recovery` the page's websocket stays open while its session does not, as lms-web's does
/// with the LMS server's: the session opens when the page says `join` and ends when it says `leave`,
/// when the SFU closes it (the page is told, `closed`) or when the SFU is gone (the page is not told:
/// its media path and its unanswered offers tell it, as they tell lms-web).
async fn session(
    rtcd: Arc<Rtcd>,
    call: String,
    probe: Probe,
    watch: Vec<String>,
    advertise: Option<(String, String)>,
    recovery: bool,
    stream: TcpStream,
) {
    let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
        return;
    };
    let (mut page_tx, mut page_rx) = socket.split();
    let name = format!("chrome-{}", probe.group);
    let mut live: Option<(RtcdConn, String)> = None;
    if !recovery {
        let Some((conn, sid, _)) = try_open_session(&rtcd, &call, &name).await else {
            return;
        };
        live = Some((conn, sid));
    }
    loop {
        let step = tokio::select! {
            m = page_rx.next() => Ok(m),
            m = next_from_sfu(&mut live) => Err(m),
        };
        match step {
            Ok(m) => {
                let Some(Ok(WsMessage::Text(text))) = m else {
                    break;
                };
                let Ok(v) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let text_of = |k: &str| v[k].as_str().unwrap_or_default().as_bytes().to_vec();
                match v["t"].as_str() {
                    Some("join") => {
                        leave(&mut live).await;
                        match try_open_session(&rtcd, &call, &name).await {
                            Some((conn, sid, _)) => live = Some((conn, sid)),
                            None => {
                                probe.event(&name, "error", "the SFU did not accept the websocket");
                                let closed = json!({ "t": "closed" }).to_string();
                                if page_tx.send(WsMessage::text(closed)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some("leave") => leave(&mut live).await,
                    Some("sdp") => {
                        if let Some((conn, sid)) = &live {
                            conn.send(message(sid, &call, MessageType::Sdp, text_of("data")));
                        }
                    }
                    Some("ice") => {
                        if let Some((conn, sid)) = &live {
                            conn.send(message(sid, &call, MessageType::Ice, text_of("data")));
                        }
                    }
                    Some("subscribe") => {
                        if let Some((conn, sid)) = &live {
                            conn.send(message(
                                sid,
                                &call,
                                MessageType::VideoSubscribe,
                                json!({ "session_ids": watch }).to_string().into_bytes(),
                            ));
                        }
                    }
                    Some("recv") => {
                        if let Some(id) = v["id"].as_str() {
                            probe.recv(id, with_delay(&probe, id, &v));
                        }
                    }
                    Some("quality") => {
                        if let Some(id) = v["id"].as_str() {
                            probe.quality(id, v["fields"].clone());
                        }
                    }
                    Some("rejoin") => probe.rejoined(
                        &name,
                        v["ms"].as_u64().unwrap_or_default(),
                        v["tries"].as_u64().unwrap_or_default() as u32,
                    ),
                    Some("event") => probe.event(
                        &name,
                        v["what"].as_str().unwrap_or("error"),
                        v["detail"].as_str().unwrap_or_default(),
                    ),
                    _ => {}
                }
            }
            Err(m) => {
                let Some((_, sid)) = &live else { continue };
                match m {
                    Some(ClientMessage::Rtc(m)) if m.session_id == *sid => {
                        let t = match m.kind {
                            MessageType::Sdp => "sdp",
                            MessageType::Ice => "ice",
                            _ => continue,
                        };
                        let mut data = String::from_utf8_lossy(&m.data).into_owned();
                        if let Some((private, public)) = &advertise {
                            data = data.replace(private.as_str(), public);
                        }
                        if page_tx
                            .send(WsMessage::text(json!({ "t": t, "data": data }).to_string()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Some(ClientMessage::Close(r)) if r.session_id == *sid => {
                        probe.event(&name, "error", "the SFU closed the session");
                        if !recovery {
                            break;
                        }
                        live = None;
                        let closed = json!({ "t": "closed" }).to_string();
                        if page_tx.send(WsMessage::text(closed)).await.is_err() {
                            break;
                        }
                    }
                    Some(_) => {}
                    None => {
                        probe.event(&name, "error", "the signaling connection closed");
                        if !recovery {
                            break;
                        }
                        live = None;
                    }
                }
            }
        }
    }
    leave(&mut live).await;
}

/// Ends the page's session on the SFU, if it has one.
async fn leave(live: &mut Option<(RtcdConn, String)>) {
    if let Some((conn, session_id)) = live.take() {
        conn.send(ClientMessage::Leave(SessionRef { session_id }));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{RtpClock, Sink};

    #[test]
    fn a_pages_playout_times_are_mapped_to_the_senders_clock() {
        let dir = std::env::temp_dir().join("lms-sim-chrome");
        std::fs::create_dir_all(&dir).unwrap();
        let _ = std::fs::remove_file(dir.join("rtp.clocks"));
        let scratch = dir.join("out.jsonl");
        let probe = Probe {
            sink: Sink::open(&scratch).unwrap(),
            imp: "rtcd-rs/chrome".into(),
            group: "clean",
            teacher: "session-teacher".into(),
            measure: false,
            leader: false,
        };
        probe.clock(
            "voice",
            "teacher",
            0,
            RtpClock {
                ts: 1000,
                wall_ms: 5_000.0,
                rate: 48_000.0,
            },
        );
        let row = json!({
            "fields": { "media": "audio" },
            // 960 ticks (20 ms) after the anchor, played 80 ms after it: 60 ms; and 100 ms.
            "sync": [{ "rtp": 1960, "at": 5_080.0 }, { "rtp": 1000, "at": 5_100.0 }],
            "ms": [],
        });
        let f = with_delay(&probe, "chrome-clean/voice:teacher", &row);
        assert_eq!(f["delay_p50_ms"], 60.0);
        assert_eq!(f["delay_p95_ms"], 100.0);
        assert_eq!(f["media"], "audio");
        // A stream no sender of this process clocks has no delay.
        let f = with_delay(&probe, "chrome-clean/video:s01", &row);
        assert!(f["delay_p50_ms"].is_null());
    }
}
