//! The session recorder.

pub mod api;
pub mod call;
pub mod config;
pub mod disk;
pub mod logging;
pub mod media;
pub mod peer;
pub mod reorder;
pub mod session;
pub mod socket;
pub mod tap;
pub mod upload;
pub mod vp8;
pub mod webm;

#[cfg(test)]
mod testing;
