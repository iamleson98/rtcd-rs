use thiserror::Error;

/// A malformed or unsupported message.
#[derive(Debug, Error)]
pub enum ProtoError {
    /// The bytes are not valid msgpack, or end early.
    #[error("invalid msgpack: {0}")]
    Msgpack(String),
    /// A value has a different msgpack type than the field needs.
    #[error("{field}: expected {expected}")]
    Type {
        field: &'static str,
        expected: &'static str,
    },
    /// The control-plane message type is not one rtcd knows.
    #[error("unknown client message type {0:?}")]
    UnknownClientMessage(String),
    /// The `rtc.Message` type number is not one rtcd knows.
    #[error("unknown rtc message type {0}")]
    UnknownMessageType(i64),
    /// The data channel frame type is not one rtcd knows.
    #[error("unknown data channel message type {0}")]
    UnknownDcMessage(u64),
    /// A compressed SDP does not inflate, or is larger than allowed.
    #[error("invalid sdp payload: {0}")]
    Sdp(String),
    /// Credentials could not be generated from these inputs.
    #[error("invalid credentials input: {0}")]
    Credentials(&'static str),
}
