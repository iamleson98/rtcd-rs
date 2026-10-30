# recorder

What a session recording is made of: the WebM files of a class call's streams, and their upload to the
LMS. It is a library; rtcd (`crates/rtcd`, `[record] enabled = true`) copies each published stream to a
writer thread that feeds it, and uploads each file as it closes. (The crate also was a standalone program
that joined the call as a listen-only bot, one job per recording; that was removed once rtcd recorded
calls itself. The crate kept its name to keep the history and the imports.)

- **Media** (`media.rs`, `webm.rs`, `vp8.rs`, `reorder.rs`): every voice, camera, shared screen and screen
  sound is written as it arrives, VP8 and Opus straight into WebM, with no decoding or re-encoding. A file
  is one continuous stretch of one person's stream (a camera from on to off, a voice while unmuted); short
  holes are bridged (audio with silence), longer ones start a new file, and a file that stays quiet is
  finished at once, not at the end. A shared screen or a camera is different: a still screen sends nothing
  for seconds at a time, and the replay shows a file only while it covers the moment, so a video file stays
  open (its last picture held) until the person turns it off, the SFU removes the track, or the call ends;
  a camera that vanishes without saying so is closed after 5 s, a screen after 30 s. A picture that changes
  size starts a file of its own with no hole, so each header tells the truth. Video files open on a
  keyframe, which the stream recorder asks for while it waits and every 20 s after, so the player seeks
  quickly. Files carry cues and a duration, so browsers seek in them. Packets that arrive out of order
  wait in a reorder ring for a retransmission (300 ms for video, 120 ms for voice), then the gap is given
  up. The length of each Opus packet is read from the packet, so any packet time is recorded right.
- **Log**: one line when a file opens and one when it closes, with why (`silent`, `the track was removed`,
  `the sender turned it off`, `the picture changed size`, `the call ended`).
- **Upload** (`upload.rs`, `api.rs`, `throttle.rs`): each file becomes an object of its own, numbered by
  its place in the recording, read straight from disk. The server says where each goes: straight to
  storage at URLs it presigns (a file over 64 MB in parts), so the bytes leave the cluster once; or, where
  storage is not directly reachable, to the server. A throttle holds all uploads of a node to one rate, so
  they never compete with media.
- **Disk** (`disk.rs`): every byte written is counted against what the recording may use; once that is
  reached it stops capturing, as filling the volume would lose the whole recording.
- **Tests** (`testing.rs`, feature `testing`): a server that plays the LMS and storage, for the tests of
  this crate and of rtcd's recording.

`cargo test -p recorder` runs the unit tests. `lms-sim run` (`crates/lms-sim`) has a real rtcd record a
class of synthetic participants and reports what it wrote and what the writer cost.
