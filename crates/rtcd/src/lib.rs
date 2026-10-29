#![cfg_attr(test, allow(clippy::field_reassign_with_default))]
//! The rtcd service: HTTP and WebSocket API, credentials, metrics and lifecycle around the SFU.
pub mod api;
pub mod app;
pub mod auth;
pub mod config;
pub mod info;
pub mod legacy;
pub mod logging;
pub mod metrics;
pub mod network;
pub mod recording;
pub mod service;
pub mod store;
pub mod tls;
pub mod ws;
