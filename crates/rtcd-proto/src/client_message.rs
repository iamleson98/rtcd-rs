use crate::ProtoError;
use crate::message::Message;
use crate::pack::{as_map, as_string, lookup_bool, lookup_string, read_value};

/// The first message on a connection, from the service to the client: who it is and which
/// connection this is (the id a reconnect refers to).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub client_id: String,
    pub conn_id: String,
}

/// A request to start a session, from the client (the Mattermost side) to the service.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JoinRequest {
    pub call_id: String,
    pub user_id: String,
    pub session_id: String,
    pub channel_id: String,
    /// The participant can send and receive AV1 screen shares.
    pub av1_support: bool,
    /// SDP after the first offer travels over the data channel instead of the websocket.
    pub dc_signaling: bool,
    /// The participant is the call's host (a class's teacher). A node that has no room left for
    /// anyone else still takes the host: without them the class cannot start, or goes on without
    /// its teacher.
    pub host: bool,
}

/// The `{"sessionID": ...}` payload of leave, reconnect and close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRef {
    pub session_id: String,
}

/// A control-plane message on the websocket between a client and the service: a msgpack string
/// naming the type, directly followed by a msgpack value holding the data (no enclosing array).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    /// Service to client, on connect.
    Hello(Hello),
    /// Client to service: start a session.
    Join(JoinRequest),
    /// Client to service: end a session.
    Leave(SessionRef),
    /// Client to service: this session's messages now go to the connection the message came on.
    Reconnect(SessionRef),
    /// Service to client: the session ended on the SFU's side.
    Close(SessionRef),
    /// Both ways: SDP, ICE and per-session notifications.
    Rtc(Message),
    /// Service to client: voice activity.
    Vad(Message),
}

impl ClientMessage {
    /// The type name on the wire, also used as a metrics label.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Hello(_) => "hello",
            Self::Join(_) => "join",
            Self::Leave(_) => "leave",
            Self::Reconnect(_) => "reconnect",
            Self::Close(_) => "close",
            Self::Rtc(_) => "rtc",
            Self::Vad(_) => "vad",
        }
    }

    /// Encodes the message for a binary websocket frame.
    pub fn pack(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(128);
        rmp::encode::write_str(&mut out, self.type_name()).expect("vec write");
        match self {
            Self::Hello(h) => write_str_map(
                &mut out,
                &[("clientID", &h.client_id), ("connID", &h.conn_id)],
            ),
            Self::Join(j) => {
                rmp::encode::write_map_len(&mut out, 7).expect("vec write");
                for (key, value) in [
                    ("callID", &j.call_id),
                    ("userID", &j.user_id),
                    ("sessionID", &j.session_id),
                    ("channelID", &j.channel_id),
                ] {
                    rmp::encode::write_str(&mut out, key).expect("vec write");
                    rmp::encode::write_str(&mut out, value).expect("vec write");
                }
                for (key, value) in [
                    ("av1Support", j.av1_support),
                    ("dcSignaling", j.dc_signaling),
                    ("host", j.host),
                ] {
                    rmp::encode::write_str(&mut out, key).expect("vec write");
                    rmp::encode::write_bool(&mut out, value).expect("vec write");
                }
            }
            Self::Leave(s) | Self::Reconnect(s) | Self::Close(s) => {
                write_str_map(&mut out, &[("sessionID", &s.session_id)])
            }
            Self::Rtc(m) | Self::Vad(m) => m.encode(&mut out),
        }
        out
    }

    /// Decodes a binary websocket frame.
    pub fn unpack(frame: &[u8]) -> Result<Self, ProtoError> {
        let (kind, rest) = read_value(frame)?;
        let kind = as_string(&kind, "type")?;
        let (data, _) = read_value(rest)?;
        Ok(match kind.as_str() {
            "hello" => {
                let map = as_map(&data, "hello")?;
                Self::Hello(Hello {
                    client_id: lookup_string(map, "clientID"),
                    conn_id: lookup_string(map, "connID"),
                })
            }
            "join" => {
                let map = as_map(&data, "join")?;
                Self::Join(JoinRequest {
                    call_id: lookup_string(map, "callID"),
                    user_id: lookup_string(map, "userID"),
                    session_id: lookup_string(map, "sessionID"),
                    channel_id: lookup_string(map, "channelID"),
                    av1_support: lookup_bool(map, "av1Support"),
                    dc_signaling: lookup_bool(map, "dcSignaling"),
                    host: lookup_bool(map, "host"),
                })
            }
            "leave" | "reconnect" | "close" => {
                let session = SessionRef {
                    session_id: lookup_string(as_map(&data, "session reference")?, "sessionID"),
                };
                match kind.as_str() {
                    "leave" => Self::Leave(session),
                    "reconnect" => Self::Reconnect(session),
                    _ => Self::Close(session),
                }
            }
            "rtc" => Self::Rtc(Message::decode(&data)?),
            "vad" => Self::Vad(Message::decode(&data)?),
            _ => return Err(ProtoError::UnknownClientMessage(kind)),
        })
    }
}

fn write_str_map(out: &mut Vec<u8>, entries: &[(&str, &String)]) {
    rmp::encode::write_map_len(out, entries.len() as u32).expect("vec write");
    for (key, value) in entries {
        rmp::encode::write_str(out, key).expect("vec write");
        rmp::encode::write_str(out, value).expect("vec write");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MessageType;

    fn message(kind: MessageType, data: &[u8]) -> Message {
        Message {
            group_id: "g".into(),
            user_id: "u".into(),
            session_id: "s".into(),
            call_id: "c".into(),
            kind,
            data: data.to_vec(),
        }
    }

    #[test]
    fn every_message_survives_a_round_trip() {
        let all = [
            ClientMessage::Hello(Hello {
                client_id: "a".into(),
                conn_id: "b".into(),
            }),
            ClientMessage::Join(JoinRequest {
                call_id: "c".into(),
                user_id: "u".into(),
                session_id: "s".into(),
                channel_id: "ch".into(),
                av1_support: true,
                dc_signaling: false,
                host: true,
            }),
            ClientMessage::Leave(SessionRef {
                session_id: "s".into(),
            }),
            ClientMessage::Reconnect(SessionRef {
                session_id: "s".into(),
            }),
            ClientMessage::Close(SessionRef {
                session_id: "s".into(),
            }),
            ClientMessage::Rtc(message(MessageType::Sdp, b"{}")),
            ClientMessage::Rtc(message(MessageType::Mute, b"")),
            ClientMessage::Vad(message(MessageType::VoiceOn, b"")),
        ];
        for msg in all {
            assert_eq!(ClientMessage::unpack(&msg.pack()).unwrap(), msg);
        }
    }

    #[test]
    fn an_unknown_type_is_named_in_the_error() {
        let mut frame = Vec::new();
        rmp::encode::write_str(&mut frame, "teleport").unwrap();
        rmp::encode::write_nil(&mut frame).unwrap();
        match ClientMessage::unpack(&frame) {
            Err(ProtoError::UnknownClientMessage(kind)) => assert_eq!(kind, "teleport"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn truncated_frames_are_errors_not_panics() {
        let frame = ClientMessage::Rtc(message(MessageType::Sdp, &[7; 300])).pack();
        for len in 0..frame.len() {
            assert!(
                ClientMessage::unpack(&frame[..len]).is_err(),
                "prefix of {len} bytes decoded"
            );
        }
    }

    #[test]
    fn a_missing_join_field_is_empty_like_the_go_decoder() {
        let mut frame = Vec::new();
        rmp::encode::write_str(&mut frame, "join").unwrap();
        rmp::encode::write_map_len(&mut frame, 1).unwrap();
        rmp::encode::write_str(&mut frame, "callID").unwrap();
        rmp::encode::write_str(&mut frame, "c1").unwrap();
        let ClientMessage::Join(join) = ClientMessage::unpack(&frame).unwrap() else {
            panic!("not a join")
        };
        assert_eq!(join.call_id, "c1");
        assert_eq!(join.session_id, "");
        assert!(!join.dc_signaling);
    }

    #[test]
    fn a_message_type_out_of_range_is_rejected() {
        let mut frame = Vec::new();
        rmp::encode::write_str(&mut frame, "rtc").unwrap();
        rmp::encode::write_map_len(&mut frame, 1).unwrap();
        rmp::encode::write_str(&mut frame, "type").unwrap();
        rmp::encode::write_uint(&mut frame, 99).unwrap();
        assert!(matches!(
            ClientMessage::unpack(&frame),
            Err(ProtoError::UnknownMessageType(99))
        ));
    }
}
