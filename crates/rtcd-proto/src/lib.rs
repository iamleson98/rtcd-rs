//! Wire formats of rtcd.
//!
//! Everything a client of the original Go `rtcd` sends or receives has its encoding here, free of
//! I/O, so the service, the SFU and the test clients agree on bytes: the control-plane messages
//! exchanged over the WebSocket ([`ClientMessage`]), the signaling frames on the data channel
//! ([`dc`]), the session description returned by the HTTP API ([`SessionConfig`]) and the TURN
//! credentials handed to browsers ([`turn`]).
//!
//! The encodings are checked against vectors produced by the Go implementation itself
//! (`compat/golden`), not against this crate's own reading of the format.

mod client_message;
pub mod dc;
mod error;
mod message;
mod pack;
mod session;
pub mod turn;

pub use client_message::{
    ClientMessage, Hello, JoinRequest, RecordEnded, RecordFile, RecordStart, RecordStop, SessionRef,
};
pub use error::ProtoError;
pub use message::{Message, MessageType};
pub use session::{SessionConfig, SessionProps};
