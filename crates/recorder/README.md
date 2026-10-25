# recorder

The LMS session recorder. One short-lived job per recording, launched by the server
(`server/channels/app/lms/recorderjob`). It joins the class call as the recorder bot, listen-only,
through the same websocket protocol and rtcd SFU the browser uses, and records what the room saw and
heard without filming it. It is a drop-in for the Go recorder: the same environment, the same
endpoints, the same manifest, the same files, the same JSON log lines, the same `recorder version`.

- **Media** (`media.rs`, `webm.rs`, `vp8.rs`, `reorder.rs`): every voice, camera, shared screen and screen
  sound rtcd forwards is written as it arrives, VP8 and Opus straight into WebM, with no decoding or
  re-encoding. A file is one continuous stretch of one person's stream (a camera from on to off, a voice
  while unmuted); short holes are bridged (audio with silence), longer ones start a new file, and a file
  that stays quiet is finished at once, not at the end. Video files open on a keyframe, and the recorder
  asks for one every 20 s so the player seeks quickly. Files carry cues and a duration, so browsers seek in
  them.
- **Board** (`board.rs`): the whiteboard's snapshot at the start and every change after (strokes, moves,
  deletes, clears, the lock, people's pointers and the strokes they are drawing), with its time, as
  gzipped NDJSON.
- **Upload** (`upload.rs`, `api.rs`): each file becomes an object of its own, numbered by its place on
  the timeline, read straight from disk. The server says where each goes: straight to storage at URLs it
  presigns (a file over 64 MB in parts), so the bytes leave the cluster once; or, where storage is not
  directly reachable, to the server. `complete` sends the manifest that places each file on the timeline.
- **Disk** (`disk.rs`): every byte written is counted against the job's volume (`MAX_DATA_BYTES`). Within
  512 MiB of it the recorder stops capturing, as at its time limit, and uploads what it has: filling the
  volume would get the pod evicted and lose the whole recording.

## How it differs from the Go recorder

| | Go (pion) | Rust (`rtc`) |
|---|---|---|
| A video frame that is lost for good | the frames after it are written, though they refer to it | the file holds its last good picture, the sender is asked for a keyframe at once, and the next one resumes the picture. The simulator cannot show a difference: the SFU's retransmissions recover every loss it can inject on loopback, so this is a safeguard, not a measured gain |
| Packets arriving out of order | a 50-packet sample builder | a reorder ring that waits for a retransmission (300 ms for video, 120 ms for voice), then gives the gap up |
| A camera that goes quiet | its file is finished when the next frame arrives, or at the end of the recording | finished 2 s after the last frame, so a crash later loses nothing already closed |
| Who a file belongs to | read when the track appears, which can be before the server has said who it is | read when the file opens |
| Opus in WebM | no `CodecDelay` / `SeekPreRoll` | both, as the spec asks |

## Cost

Measured with `scripts/bench-recorder.py` (see it for the method) on the same synthetic classes: a real
rtcd and participants sending real VP8 and Opus, both recorders run natively on the same machine and
measured by `/usr/bin/time` from start to the end of the upload.

macOS (Apple silicon), both recorders native, one run each:

| class (people / cameras / screens, seconds) | recorder | CPU seconds | peak memory | files |
|---|---|---|---|---|
| small (3 / 2 / 1, 15 s) | Go | 0.61 | 35 MiB | 7 |
| | Rust | 0.16 | 12 MiB | 7 |
| class of 20 (20 / 12 / 1, 40 s) | Go | 6.01 | 93 MiB | 34 |
| | Rust | 1.53 | 19 MiB | 34 |

That is about 4x less CPU and 5x less memory; sampled mid-recording, the Rust recorder spends about 1% of a
core on a class of 20. Both write the same files with the same frame counts, and ffmpeg decodes all of
them without an error. The image: the binary is 4.4 MB (fat LTO, abort on panic, static musl) against
10.6 MB for the Go binary, on `scratch` in both.

Run `scripts/bench-recorder.py --go <the Go recorder's directory>` to measure on your own machine; it
prints this table for four class sizes, with the median of several runs.

## Run

Environment: `SITE_URL`, `AUTH_TOKEN` (the bot session for this recording), `RECORDING_ID`,
`MAX_DURATION_SEC`, `MAX_DATA_BYTES`; `LOG_LEVEL` is optional. SIGTERM (the server removed the job) ends
it without uploading. `RECORDER_DATA_DIR` moves the working directory from `/data`.

## Tests

`cargo test -p recorder` runs the unit tests and the end-to-end ones (`tests/e2e.rs`): a class of
synthetic participants (`crates/lms-sim`) joins a real rtcd, the recorder records it, and what it uploads
is checked with ffprobe and decoded by ffmpeg. They need ffmpeg to make the media and do nothing without
it. `lms-sim run --recorder PATH` runs any recorder binary (the Go one too) on the same class and reports
its cost and what it wrote.
