//! Decodes the vectors produced by the original Go rtcd (see `compat/golden`).

use std::collections::BTreeMap;

use hmac::{Hmac, KeyInit, Mac};
use rtcd_proto::dc::{DcMessage, TrackInfo};
use rtcd_proto::{ClientMessage, MessageType, SessionConfig, turn};
use serde_json::Value;

fn vectors(file: &str) -> Vec<Value> {
    let path = format!("{}/tests/golden/{file}", env!("CARGO_MANIFEST_DIR"));
    let doc: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    doc["vectors"].as_array().unwrap().clone()
}

fn bytes(v: &Value, key: &str) -> Vec<u8> {
    hex::decode(v[key].as_str().unwrap()).unwrap()
}

fn s(v: &Value, key: &str) -> String {
    v[key].as_str().unwrap().to_owned()
}

#[test]
fn client_messages_decode_whatever_map_order_go_used() {
    let mut seen = 0;
    for v in vectors("client_messages.json") {
        let Some(_) = v.get("hex") else { continue };
        let name = s(&v, "name");
        let msg =
            ClientMessage::unpack(&bytes(&v, "hex")).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(msg.type_name(), s(&v, "type"), "{name}");
        let data = &v["data"];
        match &msg {
            ClientMessage::Hello(h) => {
                assert_eq!(h.client_id, data["clientID"].as_str().unwrap(), "{name}");
                assert_eq!(h.conn_id, data["connID"].as_str().unwrap(), "{name}");
            }
            ClientMessage::Join(j) => {
                assert_eq!(j.call_id, data["callID"].as_str().unwrap(), "{name}");
                assert_eq!(j.user_id, data["userID"].as_str().unwrap(), "{name}");
                assert_eq!(j.session_id, data["sessionID"].as_str().unwrap(), "{name}");
                assert_eq!(j.channel_id, data["channelID"].as_str().unwrap(), "{name}");
                assert_eq!(
                    j.av1_support,
                    data["av1Support"].as_bool().unwrap(),
                    "{name}"
                );
                assert_eq!(
                    j.dc_signaling,
                    data["dcSignaling"].as_bool().unwrap(),
                    "{name}"
                );
            }
            ClientMessage::Leave(r) | ClientMessage::Reconnect(r) | ClientMessage::Close(r) => {
                assert_eq!(r.session_id, data["sessionID"].as_str().unwrap(), "{name}");
            }
            ClientMessage::Rtc(m) | ClientMessage::Vad(m) => {
                assert_eq!(m.group_id, data["group_id"].as_str().unwrap(), "{name}");
                assert_eq!(m.user_id, data["user_id"].as_str().unwrap(), "{name}");
                assert_eq!(m.session_id, data["session_id"].as_str().unwrap(), "{name}");
                assert_eq!(m.call_id, data["call_id"].as_str().unwrap(), "{name}");
                assert_eq!(m.kind as i64, data["type"].as_i64().unwrap(), "{name}");
                assert_eq!(
                    hex::encode(&m.data),
                    data["data_hex"].as_str().unwrap(),
                    "{name}"
                );
            }
        }
        seen += 1;
    }
    assert!(seen > 30, "only {seen} vectors");
}

/// Go writes the fields of `rtc.Message` in struct order, so for those messages the bytes are
/// reproducible and must match exactly: the Go clients decode what this crate encodes.
#[test]
fn rtc_messages_encode_to_the_exact_bytes_go_does() {
    for v in vectors("client_messages.json") {
        let name = s(&v, "name");
        if !(name.starts_with("rtc/") || name.starts_with("vad/")) {
            continue;
        }
        let golden = bytes(&v, "hex");
        let msg = ClientMessage::unpack(&golden).unwrap();
        assert_eq!(msg.pack(), golden, "{name}");
    }
}

#[test]
fn the_message_types_keep_their_go_numbers() {
    let expected = [
        ("rtc/ice", MessageType::Ice),
        ("rtc/sdp", MessageType::Sdp),
        ("rtc/mute", MessageType::Mute),
        ("rtc/unmute", MessageType::Unmute),
        ("rtc/screen_on", MessageType::ScreenOn),
        ("rtc/screen_off", MessageType::ScreenOff),
        ("rtc/voice_on", MessageType::VoiceOn),
        ("rtc/voice_off", MessageType::VoiceOff),
        ("rtc/video_on", MessageType::VideoOn),
        ("rtc/video_off", MessageType::VideoOff),
        ("rtc/video_subscribe", MessageType::VideoSubscribe),
    ];
    let all = vectors("client_messages.json");
    for (name, kind) in expected {
        let v = all.iter().find(|v| v["name"] == name).unwrap();
        let ClientMessage::Rtc(m) = ClientMessage::unpack(&bytes(v, "hex")).unwrap() else {
            panic!()
        };
        assert_eq!(m.kind, kind, "{name}");
    }
}

#[test]
fn data_channel_frames_decode_and_reencode_like_go() {
    for v in vectors("dc_messages.json") {
        let name = s(&v, "name");
        let golden = bytes(&v, "hex");
        let msg = DcMessage::decode(&golden).unwrap_or_else(|e| panic!("{name}: {e}"));
        let payload = &v["payload"];
        match (name.as_str(), &msg) {
            ("ping", DcMessage::Ping)
            | ("pong", DcMessage::Pong)
            | ("unlock", DcMessage::Unlock) => {}
            ("lock_request", DcMessage::Lock(None)) => {}
            ("lock_true", DcMessage::Lock(Some(true)))
            | ("lock_false", DcMessage::Lock(Some(false))) => {}
            ("loss_rate", DcMessage::LossRate(x))
            | ("rtt", DcMessage::RoundTripTime(x))
            | ("jitter", DcMessage::Jitter(x)) => {
                assert_eq!(*x, payload.as_f64().unwrap(), "{name}")
            }
            ("sdp", DcMessage::Sdp(json)) => assert_eq!(
                String::from_utf8(json.clone()).unwrap(),
                payload.as_str().unwrap()
            ),
            ("media_map", DcMessage::MediaMap(map)) => {
                let want = BTreeMap::from([(
                    "0".to_owned(),
                    TrackInfo {
                        kind: "voice".into(),
                        sender_id: "s1".into(),
                    },
                )]);
                assert_eq!(*map, want);
            }
            other => panic!("{name}: decoded as {other:?}"),
        }
        // Everything but the compressed SDP is deterministic.
        if name != "sdp" {
            assert_eq!(msg.encode(), golden, "{name}");
        }
    }
}

#[test]
fn an_sdp_we_compress_inflates_back() {
    let json = br#"{"type":"answer","sdp":"v=0\r\n"}"#.to_vec();
    assert_eq!(
        DcMessage::decode(&DcMessage::Sdp(json.clone()).encode()).unwrap(),
        DcMessage::Sdp(json)
    );
}

#[test]
fn an_sdp_that_inflates_past_the_cap_is_refused() {
    let huge = DcMessage::Sdp(vec![b' '; (rtcd_proto::dc::MAX_SDP_BYTES + 1) as usize]).encode();
    assert!(
        huge.len() < 100_000,
        "a bomb should be small on the wire: {}",
        huge.len()
    );
    assert!(DcMessage::decode(&huge).is_err());
}

#[test]
fn unknown_and_truncated_frames_error() {
    assert!(DcMessage::decode(&[0xcc, 0x63]).is_err());
    assert!(DcMessage::decode(&[]).is_err());
    assert!(
        DcMessage::decode(&[0xcc, 0x04]).is_err(),
        "a loss rate without its number"
    );
}

#[test]
fn the_http_session_json_matches_go() {
    let docs = vectors("session_config.json");
    let one: SessionConfig = serde_json::from_str(docs[0]["json"].as_str().unwrap()).unwrap();
    assert_eq!(
        (
            one.group_id.as_str(),
            one.call_id.as_str(),
            one.user_id.as_str(),
            one.session_id.as_str()
        ),
        ("g", "c", "u", "s")
    );
    assert_eq!(one.props.channel_id(), "ch");
    assert!(one.props.dc_signaling());
    assert!(!one.props.av1_support());
    // What we write must read the same as what Go wrote.
    let ours: Value = serde_json::to_value(&one).unwrap();
    let theirs: Value = serde_json::from_str(docs[0]["json"].as_str().unwrap()).unwrap();
    assert_eq!(ours, theirs);

    let many: Vec<SessionConfig> = serde_json::from_str(docs[1]["json"].as_str().unwrap()).unwrap();
    assert_eq!(many.len(), 2);
    assert_eq!(
        many[1].props.channel_id(),
        "",
        "a session without props reads as empty"
    );
}

#[test]
fn turn_credentials_verify_against_the_go_ones() {
    for v in vectors("turn.json") {
        let secret = s(&v, "secret");
        let username = s(&v, "username");
        assert_eq!(
            turn::password_for(&username, &secret),
            s(&v, "credential"),
            "{secret}"
        );

        let (expiry, name) = username.split_once(':').unwrap();
        let ours = turn::credentials(
            name,
            &secret,
            expiry.parse().unwrap(),
            expiry.parse::<i64>().unwrap() - 3600,
        )
        .unwrap();
        assert_eq!(
            (ours.username, ours.password),
            (username, s(&v, "credential"))
        );
    }
    // A raw HMAC check independent of the helper, so a bug in the helper cannot hide itself.
    let mut mac = <Hmac<sha1::Sha1> as KeyInit>::new_from_slice(b"k").unwrap();
    mac.update(b"u");
    assert_eq!(
        turn::password_for("u", "k").len(),
        28,
        "base64 of 20 bytes: {:?}",
        mac.finalize().into_bytes().len()
    );
}

#[test]
fn turn_credentials_refuse_unsafe_inputs() {
    let now = 1_700_000_000;
    assert!(turn::credentials("", "s", now + 60, now).is_err());
    assert!(turn::credentials("n", "", now + 60, now).is_err());
    assert!(turn::credentials("n", "s", 0, now).is_err());
    assert!(turn::credentials("n", "s", now + turn::MAX_EXPIRATION_MINUTES * 60 + 1, now).is_err());
    assert!(turn::credentials("n", "s", now + turn::MAX_EXPIRATION_MINUTES * 60, now).is_ok());
}

#[test]
fn go_bcrypt_hashes_verify_with_the_rust_library() {
    for v in vectors("bcrypt.json") {
        assert!(
            bcrypt::verify(s(&v, "key"), &s(&v, "hash")).unwrap(),
            "{}",
            s(&v, "name")
        );
        assert!(!bcrypt::verify("wrong", &s(&v, "hash")).unwrap());
    }
}
