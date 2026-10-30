use std::fmt;

use crate::ProtoError;
use crate::message::Message;
use crate::pack::{
    as_map, as_string, lookup_bool, lookup_i64, lookup_string, lookup_u32, lookup_u64, read_value,
};

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
    /// The participant only watches the call: the session recorder. A node with no room left
    /// still takes it, or the class would not be recorded.
    pub observer: bool,
}

/// The `{"sessionID": ...}` payload of leave, reconnect and close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRef {
    pub session_id: String,
}

/// Server to service: record this call. The service numbers the files it writes from
/// `file_base` and sends the bot's `token` back to the server with each upload.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct RecordStart {
    pub recording_id: String,
    pub call_id: String,
    /// The recording's start, in epoch milliseconds: every file is placed on the timeline by its
    /// offset from it.
    pub t0: i64,
    /// The index the first file is numbered from, so that nodes and restarts never reuse one.
    pub file_base: u32,
    /// The most files this start may write: the run ends (`file_limit`) once it has numbered
    /// `file_base + max_files` of them, so that it never takes an index of the range beyond. Zero,
    /// which an older server sends, is no limit.
    pub max_files: u32,
    /// Where files are uploaded to.
    pub site_url: String,
    /// Authenticates the uploads. Never printed or logged.
    pub token: String,
}

impl fmt::Debug for RecordStart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordStart")
            .field("recording_id", &self.recording_id)
            .field("call_id", &self.call_id)
            .field("t0", &self.t0)
            .field("file_base", &self.file_base)
            .field("max_files", &self.max_files)
            .field("site_url", &self.site_url)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Server to service: stop recording, finishing and uploading what is open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordStop {
    pub recording_id: String,
}

/// Service to server: a file of the recording is uploaded, whole.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordFile {
    pub recording_id: String,
    pub index: u32,
    /// What the file holds (camera, screen, voice, ...), as the recorder names it.
    pub kind: String,
    pub session_id: String,
    pub user_id: String,
    /// Where the file begins and ends on the recording's timeline, in milliseconds.
    pub start_ms: i64,
    pub end_ms: i64,
    /// The picture size; zero for audio.
    pub width: u32,
    pub height: u32,
    /// Bytes uploaded.
    pub size: u64,
}

/// Service to server: the recording is over on this node.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordEnded {
    pub recording_id: String,
    /// Why: `stopped`, `call_ended`, `disk_full`, `file_limit`, `failed` or `refused`. A start
    /// that was not taken (the node is draining, the recording is held for another call or
    /// number, the call has another recording) is answered `refused` at once, with no files; one
    /// that was taken is answered once, when its capture is over and its files are in.
    pub reason: String,
    /// The index the first file was numbered from (the `file_base` of the start).
    pub file_base: u32,
    /// How many indexes the files took, counting from `file_base`: the highest index used, plus
    /// one, minus `file_base`. A file that could not be uploaded leaves a hole in the numbering.
    pub files: u32,
    /// How long the capture lasted, from the recording's start (`t0`) to the moment it ended,
    /// in milliseconds.
    pub duration_ms: i64,
    /// Packets that were not recorded because the writer could not keep up.
    pub dropped: u64,
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
    /// Server to service: start recording a call.
    RecordStart(RecordStart),
    /// Server to service: stop recording.
    RecordStop(RecordStop),
    /// Service to server: a file is uploaded.
    RecordFile(RecordFile),
    /// Service to server: the recording ended on this node.
    RecordEnded(RecordEnded),
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
            Self::RecordStart(_) => "record_start",
            Self::RecordStop(_) => "record_stop",
            Self::RecordFile(_) => "record_file",
            Self::RecordEnded(_) => "record_ended",
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
                rmp::encode::write_map_len(&mut out, 8).expect("vec write");
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
                    ("observer", j.observer),
                ] {
                    rmp::encode::write_str(&mut out, key).expect("vec write");
                    rmp::encode::write_bool(&mut out, value).expect("vec write");
                }
            }
            Self::Leave(s) | Self::Reconnect(s) | Self::Close(s) => {
                write_str_map(&mut out, &[("sessionID", &s.session_id)])
            }
            Self::Rtc(m) | Self::Vad(m) => m.encode(&mut out),
            Self::RecordStart(r) => {
                rmp::encode::write_map_len(&mut out, 7).expect("vec write");
                write_str_entry(&mut out, "recordingID", &r.recording_id);
                write_str_entry(&mut out, "callID", &r.call_id);
                write_int_entry(&mut out, "t0", r.t0);
                write_uint_entry(&mut out, "fileBase", r.file_base.into());
                write_uint_entry(&mut out, "maxFiles", r.max_files.into());
                write_str_entry(&mut out, "siteURL", &r.site_url);
                write_str_entry(&mut out, "token", &r.token);
            }
            Self::RecordStop(r) => write_str_map(&mut out, &[("recordingID", &r.recording_id)]),
            Self::RecordFile(r) => {
                rmp::encode::write_map_len(&mut out, 10).expect("vec write");
                write_str_entry(&mut out, "recordingID", &r.recording_id);
                write_uint_entry(&mut out, "index", r.index.into());
                write_str_entry(&mut out, "kind", &r.kind);
                write_str_entry(&mut out, "sessionID", &r.session_id);
                write_str_entry(&mut out, "userID", &r.user_id);
                write_int_entry(&mut out, "startMs", r.start_ms);
                write_int_entry(&mut out, "endMs", r.end_ms);
                write_uint_entry(&mut out, "width", r.width.into());
                write_uint_entry(&mut out, "height", r.height.into());
                write_uint_entry(&mut out, "size", r.size);
            }
            Self::RecordEnded(r) => {
                rmp::encode::write_map_len(&mut out, 6).expect("vec write");
                write_str_entry(&mut out, "recordingID", &r.recording_id);
                write_str_entry(&mut out, "reason", &r.reason);
                write_uint_entry(&mut out, "fileBase", r.file_base.into());
                write_uint_entry(&mut out, "files", r.files.into());
                write_int_entry(&mut out, "durationMs", r.duration_ms);
                write_uint_entry(&mut out, "dropped", r.dropped);
            }
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
                    observer: lookup_bool(map, "observer"),
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
            "record_start" => {
                let map = as_map(&data, "record_start")?;
                Self::RecordStart(RecordStart {
                    recording_id: lookup_string(map, "recordingID"),
                    call_id: lookup_string(map, "callID"),
                    t0: lookup_i64(map, "t0"),
                    file_base: lookup_u32(map, "fileBase"),
                    max_files: lookup_u32(map, "maxFiles"),
                    site_url: lookup_string(map, "siteURL"),
                    token: lookup_string(map, "token"),
                })
            }
            "record_stop" => Self::RecordStop(RecordStop {
                recording_id: lookup_string(as_map(&data, "record_stop")?, "recordingID"),
            }),
            "record_file" => {
                let map = as_map(&data, "record_file")?;
                Self::RecordFile(RecordFile {
                    recording_id: lookup_string(map, "recordingID"),
                    index: lookup_u32(map, "index"),
                    kind: lookup_string(map, "kind"),
                    session_id: lookup_string(map, "sessionID"),
                    user_id: lookup_string(map, "userID"),
                    start_ms: lookup_i64(map, "startMs"),
                    end_ms: lookup_i64(map, "endMs"),
                    width: lookup_u32(map, "width"),
                    height: lookup_u32(map, "height"),
                    size: lookup_u64(map, "size"),
                })
            }
            "record_ended" => {
                let map = as_map(&data, "record_ended")?;
                Self::RecordEnded(RecordEnded {
                    recording_id: lookup_string(map, "recordingID"),
                    reason: lookup_string(map, "reason"),
                    file_base: lookup_u32(map, "fileBase"),
                    files: lookup_u32(map, "files"),
                    duration_ms: lookup_i64(map, "durationMs"),
                    dropped: lookup_u64(map, "dropped"),
                })
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

fn write_str_entry(out: &mut Vec<u8>, key: &str, value: &str) {
    rmp::encode::write_str(out, key).expect("vec write");
    rmp::encode::write_str(out, value).expect("vec write");
}

fn write_int_entry(out: &mut Vec<u8>, key: &str, value: i64) {
    rmp::encode::write_str(out, key).expect("vec write");
    rmp::encode::write_sint(out, value).expect("vec write");
}

fn write_uint_entry(out: &mut Vec<u8>, key: &str, value: u64) {
    rmp::encode::write_str(out, key).expect("vec write");
    rmp::encode::write_uint(out, value).expect("vec write");
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

    fn record_start() -> RecordStart {
        RecordStart {
            recording_id: "rec1".into(),
            call_id: "c".into(),
            t0: 1_793_000_000_123,
            file_base: 100,
            max_files: 20_000,
            site_url: "https://lms.example".into(),
            token: "s3cr3t-token".into(),
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
                observer: true,
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
            ClientMessage::RecordStart(record_start()),
            ClientMessage::RecordStop(RecordStop {
                recording_id: "rec1".into(),
            }),
            ClientMessage::RecordFile(RecordFile {
                recording_id: "rec1".into(),
                index: 7,
                kind: "camera".into(),
                session_id: "s".into(),
                user_id: "u".into(),
                start_ms: 1_500,
                end_ms: 61_500,
                width: 1280,
                height: 720,
                size: 5_000_000_000,
            }),
            ClientMessage::RecordEnded(RecordEnded {
                recording_id: "rec1".into(),
                reason: "call_ended".into(),
                file_base: 100,
                files: 12,
                duration_ms: 3_600_000,
                dropped: 42,
            }),
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

    #[test]
    fn the_recording_messages_keep_the_keys_and_names_of_the_design() {
        let frame = ClientMessage::RecordFile(RecordFile {
            recording_id: "r".into(),
            index: 1,
            kind: "voice".into(),
            session_id: "s".into(),
            user_id: "u".into(),
            start_ms: -5,
            end_ms: 9,
            width: 0,
            height: 0,
            size: 3,
        })
        .pack();
        let (kind, rest) = read_value(&frame).unwrap();
        assert_eq!(kind.as_str(), Some("record_file"));
        let (data, _) = read_value(rest).unwrap();
        let mut keys: Vec<&str> = as_map(&data, "record_file")
            .unwrap()
            .iter()
            .map(|(k, _)| k.as_str().unwrap())
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "endMs",
                "height",
                "index",
                "kind",
                "recordingID",
                "sessionID",
                "size",
                "startMs",
                "userID",
                "width"
            ]
        );
        for (message, name) in [
            (ClientMessage::RecordStart(record_start()), "record_start"),
            (
                ClientMessage::RecordStop(RecordStop {
                    recording_id: "r".into(),
                }),
                "record_stop",
            ),
            (
                ClientMessage::RecordEnded(RecordEnded::default()),
                "record_ended",
            ),
        ] {
            assert_eq!(message.type_name(), name);
        }
    }

    #[test]
    fn a_record_message_reads_any_integer_width_and_ignores_unknown_fields() {
        let mut frame = Vec::new();
        rmp::encode::write_str(&mut frame, "record_ended").unwrap();
        rmp::encode::write_map_len(&mut frame, 3).unwrap();
        rmp::encode::write_str(&mut frame, "recordingID").unwrap();
        rmp::encode::write_str(&mut frame, "r1").unwrap();
        // A Go encoder writes a uint64 as the smallest width; a newer peer adds a field.
        rmp::encode::write_str(&mut frame, "dropped").unwrap();
        rmp::encode::write_u8(&mut frame, 3).unwrap();
        rmp::encode::write_str(&mut frame, "later").unwrap();
        rmp::encode::write_bool(&mut frame, true).unwrap();
        let ClientMessage::RecordEnded(ended) = ClientMessage::unpack(&frame).unwrap() else {
            panic!("not a record_ended")
        };
        assert_eq!(ended.recording_id, "r1");
        assert_eq!(ended.dropped, 3);
        assert_eq!(ended.reason, "", "a missing field is empty, as for join");
    }

    #[test]
    fn a_record_start_with_missing_or_out_of_range_numbers_reads_them_as_zero() {
        let mut frame = Vec::new();
        rmp::encode::write_str(&mut frame, "record_start").unwrap();
        rmp::encode::write_map_len(&mut frame, 2).unwrap();
        rmp::encode::write_str(&mut frame, "recordingID").unwrap();
        rmp::encode::write_str(&mut frame, "r1").unwrap();
        rmp::encode::write_str(&mut frame, "fileBase").unwrap();
        rmp::encode::write_uint(&mut frame, u64::from(u32::MAX) + 1).unwrap();
        let ClientMessage::RecordStart(start) = ClientMessage::unpack(&frame).unwrap() else {
            panic!("not a record_start")
        };
        assert_eq!(start.recording_id, "r1");
        assert_eq!((start.t0, start.file_base), (0, 0));
        assert_eq!(start.token, "");
    }

    #[test]
    fn a_record_start_without_a_file_limit_is_one_without_a_limit() {
        // What a server from before the limit sends: no `maxFiles` at all.
        let mut frame = Vec::new();
        rmp::encode::write_str(&mut frame, "record_start").unwrap();
        rmp::encode::write_map_len(&mut frame, 2).unwrap();
        rmp::encode::write_str(&mut frame, "recordingID").unwrap();
        rmp::encode::write_str(&mut frame, "r1").unwrap();
        rmp::encode::write_str(&mut frame, "fileBase").unwrap();
        rmp::encode::write_uint(&mut frame, 40_000).unwrap();
        let ClientMessage::RecordStart(start) = ClientMessage::unpack(&frame).unwrap() else {
            panic!("not a record_start")
        };
        assert_eq!((start.file_base, start.max_files), (40_000, 0));
    }

    #[test]
    fn the_upload_token_is_not_printed() {
        let start = record_start();
        for text in [
            format!("{start:?}"),
            format!("{start:#?}"),
            format!("{:?}", ClientMessage::RecordStart(start.clone())),
        ] {
            assert!(!text.contains(&start.token), "{text}");
            assert!(text.contains("<redacted>"), "{text}");
            assert!(text.contains(&start.recording_id), "{text}");
        }
    }
}
