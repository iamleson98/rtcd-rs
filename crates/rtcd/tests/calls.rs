//! The websocket protocol and calls through it, end to end: msgpack messages in, browsers on real
//! UDP sockets, voice out.

mod common;

use std::time::Duration;

use common::{KEY, Participant, TestServer};
use rtcd_proto::{
    ClientMessage, Message, MessageType, RecordStart, RecordStop, SessionConfig, SessionRef,
};
use serde_json::Value;

async fn sessions(s: &TestServer, client: &str, call: &str) -> (u16, Value) {
    let r = s
        .http
        .get(s.url(&format!("/calls/{call}/sessions")))
        .header("Authorization", TestServer::basic(client, KEY))
        .send()
        .await
        .unwrap();
    let code = r.status().as_u16();
    (
        code,
        serde_json::from_str(&r.text().await.unwrap()).unwrap_or(Value::Null),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_is_greeted_with_its_identity() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let ws = s.connect("mm").await;
    assert_eq!(ws.client_id, "mm");
    assert_eq!(
        ws.conn_id.len(),
        26,
        "clients check the length of the connection id"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bearer_token_opens_a_connection_too() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let r = s
        .http
        .post(s.url("/login"))
        .json(&serde_json::json!({ "clientID": "mm", "authKey": KEY }))
        .send()
        .await
        .unwrap();
    let token = r.json::<Value>().await.unwrap()["bearerToken"]
        .as_str()
        .unwrap()
        .to_owned();
    let ws = s.connect_with(&format!("Bearer {token}")).await.unwrap();
    assert_eq!(ws.client_id, "mm");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn connections_without_valid_credentials_are_refused() {
    let s = TestServer::start().await;
    s.register("mm").await;
    assert!(
        s.connect_with(&TestServer::basic("mm", "wrong-key-wrong-key-wrong-key-xx"))
            .await
            .is_err()
    );
    assert!(
        s.connect_with(&TestServer::basic("nobody", KEY))
            .await
            .is_err()
    );
    assert!(s.connect_with("Bearer not-a-token").await.is_err());
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn two_participants_hear_each_other_through_the_service() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let (mut wa, mut wb) = (s.connect("mm").await, s.connect("mm").await);
    let mut a = Participant::join(&mut wa, s.media, "alice", "call1", 7001).await;
    let mut b = Participant::join(&mut wb, s.media, "bob", "call1", 7002).await;

    for _ in 0..2 {
        a.speak(&mut wa, 50).await;
        b.speak(&mut wb, 50).await;
    }
    let end = tokio::time::Instant::now() + Duration::from_secs(3);
    while (a.browser.received("voice_session-bob") < 40
        || b.browser.received("voice_session-alice") < 40)
        && tokio::time::Instant::now() < end
    {
        a.pump(&mut wa).await;
        b.pump(&mut wb).await;
        a.speak(&mut wa, 5).await;
        b.speak(&mut wb, 5).await;
    }
    assert!(
        b.browser.received("voice_session-alice") >= 40,
        "bob heard {}",
        b.browser.received("voice_")
    );
    assert!(
        a.browser.received("voice_session-bob") >= 40,
        "alice heard {}",
        a.browser.received("voice_")
    );
    assert!(
        a.voice.contains(&true),
        "voice activity is reported to the client as vad messages: {:?}",
        a.voice
    );

    // The HTTP API sees the call, in the client's group.
    let (code, v) = sessions(&s, "mm", "call1").await;
    assert_eq!(code, 200);
    let ids: Vec<&str> = v
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["SessionID"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"session-alice") && ids.contains(&"session-bob"));
    assert_eq!(v[0]["GroupID"], "mm");
    let one = s
        .http
        .get(s.url("/calls/call1/sessions/session-alice"))
        .header("Authorization", TestServer::basic("mm", KEY))
        .send()
        .await
        .unwrap();
    assert_eq!(one.status(), 200);
    assert_eq!(
        one.json::<Value>().await.unwrap()["Props"]["dcSignaling"],
        true
    );

    let metrics = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("rtcd_rtc_sessions_total{groupID=\"mm\"} 2"),
        "{metrics}"
    );
    assert!(metrics.contains("rtcd_ws_connections_total{clientID=\"mm\"} 2"));
    assert!(metrics.contains("rtcd_rtc_rtp_bytes_sent_total{type=\"voice\"}"));
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn leaving_ends_the_session_and_the_client_is_told() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let mut ws = s.connect("mm").await;
    let mut a = Participant::join(&mut ws, s.media, "alice", "call1", 7001).await;
    ws.leave("session-alice").await;
    let end = tokio::time::Instant::now() + Duration::from_secs(5);
    while !a.closed && tokio::time::Instant::now() < end {
        a.pump(&mut ws).await;
    }
    assert!(a.closed, "a close message names the session");
    assert_eq!(sessions(&s, "mm", "call1").await.0, 404);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_cannot_act_on_another_clients_session() {
    let s = TestServer::start().await;
    s.register("mm").await;
    s.register("other").await;
    let mut mine = s.connect("mm").await;
    let mut theirs = s.connect("other").await;
    let _a = Participant::join(&mut mine, s.media, "alice", "call1", 7001).await;

    // The other client names alice's session in a leave, a message and a reconnect.
    theirs.leave("session-alice").await;
    theirs
        .send(&ClientMessage::Rtc(Message {
            group_id: "default".into(),
            user_id: "u".into(),
            session_id: "session-alice".into(),
            call_id: "call1".into(),
            kind: MessageType::Mute,
            data: vec![],
        }))
        .await;
    theirs
        .send(&ClientMessage::Reconnect(SessionRef {
            session_id: "session-alice".into(),
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        sessions(&s, "mm", "call1").await.0,
        200,
        "the session is still there"
    );
    assert!(
        theirs.try_next(Duration::from_millis(200)).await.is_none(),
        "and nothing was sent to the other client"
    );

    // Nor can it see it.
    let r = s
        .http
        .get(s.url("/calls/call1/sessions"))
        .header("Authorization", TestServer::basic("other", KEY))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnecting_client_moves_its_sessions_to_the_new_connection() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let mut first = s.connect("mm").await;
    let mut a = Participant::join(&mut first, s.media, "alice", "call1", 7001).await;
    let mut b_ws = s.connect("mm").await;
    let _b = Participant::join(&mut b_ws, s.media, "bob", "call1", 7002).await;

    // The websocket drops; the client comes back on a new one and says which session is its.
    drop(first);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut second = s.connect("mm").await;
    second
        .send(&ClientMessage::Reconnect(SessionRef {
            session_id: "session-alice".into(),
        }))
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Voice activity, which the SFU reports unprompted, now arrives on the new connection.
    let end = tokio::time::Instant::now() + Duration::from_secs(8);
    while a.voice.is_empty() && tokio::time::Instant::now() < end {
        a.speak(&mut second, 25).await;
    }
    assert!(
        a.voice.contains(&true),
        "the new connection got the vad message"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_join_the_sfu_refuses_is_closed_back_to_the_client() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let mut ws = s.connect("mm").await;
    // No call id: not a session.
    ws.send(&ClientMessage::Join(rtcd_proto::JoinRequest {
        call_id: String::new(),
        user_id: "u".into(),
        session_id: "broken".into(),
        channel_id: String::new(),
        av1_support: false,
        dc_signaling: true,
        host: false,
        observer: false,
    }))
    .await;
    match ws.next_message().await {
        ClientMessage::Close(r) => assert_eq!(r.session_id, "broken"),
        other => panic!("expected a close, got {other:?}"),
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_join_does_not_close_the_existing_session() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let mut ws = s.connect("mm").await;
    let mut a = Participant::join(&mut ws, s.media, "alice", "call1", 7001).await;
    ws.join("call1", "session-alice").await;
    for _ in 0..20 {
        a.pump(&mut ws).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!a.closed, "a second join of a live session leaves it alone");
    assert_eq!(sessions(&s, "mm", "call1").await.0, 200);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn garbage_on_the_websocket_does_not_end_the_connection() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let mut ws = s.connect("mm").await;
    ws.send(&ClientMessage::Close(SessionRef {
        session_id: "x".into(),
    }))
    .await;
    // A message the service does not accept from clients, then a real one still works.
    let mut a = Participant::join(&mut ws, s.media, "alice", "call1", 7001).await;
    a.speak(&mut ws, 5).await;
    assert_eq!(sessions(&s, "mm", "call1").await.0, 200);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn recording_messages_are_counted_and_otherwise_ignored_for_now() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let mut ws = s.connect("mm").await;
    ws.send(&ClientMessage::RecordStart(RecordStart {
        recording_id: "rec1".into(),
        call_id: "call1".into(),
        token: "not-for-logs".into(),
        ..RecordStart::default()
    }))
    .await;
    ws.send(&ClientMessage::RecordStop(RecordStop {
        recording_id: "rec1".into(),
    }))
    .await;
    // The connection and the call machinery are untouched.
    let mut a = Participant::join(&mut ws, s.media, "alice", "call1", 7001).await;
    a.speak(&mut ws, 5).await;
    assert_eq!(sessions(&s, "mm", "call1").await.0, 200);
    let metrics = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for kind in ["record_start", "record_stop"] {
        let line =
            format!("rtcd_ws_messages_total{{clientID=\"mm\",direction=\"in\",type=\"{kind}\"}} 1");
        assert!(metrics.contains(&line), "{line} not in {metrics}");
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_ends_sessions_and_closes_connections() {
    let s = TestServer::start().await;
    s.register("mm").await;
    let mut ws = s.connect("mm").await;
    let mut a = Participant::join(&mut ws, s.media, "alice", "call1", 7001).await;
    let app = {
        let mut s = s;
        let app = s.app.take().unwrap();
        // Keep the server's directory alive until the end of the test.
        std::mem::forget(s);
        app
    };
    let drained = tokio::spawn(app.shutdown(Some(Duration::from_millis(100))));
    // Generous: this runs beside other tests on shared CI machines.
    let end = tokio::time::Instant::now() + Duration::from_secs(20);
    while !a.closed && tokio::time::Instant::now() < end {
        a.pump(&mut ws).await;
    }
    assert!(a.closed, "the participant's session was ended");
    drained.await.unwrap();
    assert!(
        ws.closed(Duration::from_secs(15)).await,
        "and the websocket closed"
    );
}

#[allow(dead_code)]
fn _types(_: SessionConfig) {}
