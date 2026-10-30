//! What a session recording is made of: the WebM files of a call's streams, and their upload to the
//! LMS. rtcd records calls with it.

pub mod api;
pub mod disk;
pub mod media;
pub mod reorder;
pub mod tap;
pub mod throttle;
pub mod upload;
pub mod vp8;
pub mod webm;

#[cfg(any(test, feature = "testing"))]
pub mod testing;
