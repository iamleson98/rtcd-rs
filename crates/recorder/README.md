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
  that stays quiet is finished at once, not at the end. A shared screen or a camera is different: a still
  screen sends nothing for seconds at a time, and the replay shows a file only while it covers the moment, so
  a video file stays open (its last picture held) until the person turns it off (`user_video_off`,
  `user_screen_off`), the SFU removes the track, or the call ends; a camera that vanishes without saying so is
  closed after 5 s, a screen after 30 s. A picture that changes size (the screen moved to another layer or
  window) starts a file of its own with no hole, so each header tells the truth. Video files open on a
  keyframe, and the recorder asks for one every 20 s so the player seeks quickly. Files carry cues and a
  duration, so browsers seek in them.
- **Quality**: the shared screen is recorded on its sharp simulcast layer from the first frame (the recorder
  asks rtcd over the data channel; rtcd lets go of it only when the link is starved). Left to rtcd's bandwidth
  estimate a receiver of the small layer cannot raise the estimate, and a real class was recorded at half
  resolution for most of its length. The sharp layer is two to three times the bytes in storage and in every
  replay; `SCREEN_QUALITY=auto` in the recorder's environment leaves the choice to rtcd, as the Go recorder
  did.
- **Log**: one line when a file opens and one when it closes, with why (`silent`, `the track was removed`,
  `the sender turned it off`, `the picture changed size`, `the call ended`).
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
| A camera that goes quiet | its file is finished when the next frame arrives, or at the end of the recording | finished at once when the person turns it off, 5 s after the last frame if they never say so, so a crash later loses nothing already closed |
| A still screen share | closed after 2 s without a frame; the replay shows no screen until the next file starts (2 s and 5 s holes in a real 5-minute class) | one file, the last picture held until the share ends |
| Screen share quality | whatever size rtcd's bandwidth estimate allows: the small layer for 212 of 282 s in a real class | the sharp layer, asked for |
| A picture that changes size | the file header keeps the first size | a new file with no hole, with its own size |
| Who a file belongs to | read when the track appears, which can be before the server has said who it is | read when the file opens |
| Opus in WebM | no `CodecDelay` / `SeekPreRoll` | both, as the spec asks |

## Cost

Measured with `scripts/bench-recorder.py` (see it for the method) on the same synthetic classes: a real
rtcd and participants sending real VP8 and Opus, both recorders run natively on the same machine and
measured by `/usr/bin/time` from start to the end of the upload.

Linux (arm64 container, glibc, median of two runs; the bench ran alone):

| class (people / cameras / screens, seconds) | recorder | CPU seconds | CPU % of a core | peak memory |
|---|---|---|---|---|
| small (3 / 2 / 1, 15 s) | Go | 0.36 | 2.4% | 21.9 MiB |
| | Rust | 0.16 | 1.0% | 9.2 MiB |
| class of 10 (10 / 6 / 1, 30 s) | Go | 2.33 | 7.8% | 36.9 MiB |
| | Rust | 0.47 | 1.6% | 10.4 MiB |
| class of 20 (20 / 12 / 1, 40 s) | Go | 2.82 | 7.0% | 55.5 MiB |
| | Rust | 0.88 | 2.2% | 13.0 MiB |
| class of 30 (30 / 20 / 2, 60 s) | Go | 3.75 | 6.3% | 81.8 MiB |
| | Rust | 1.37 | 2.3% | 15.6 MiB |

The Rust recorder uses 2.4 to 5 times less CPU and 2.4 to 5 times less memory, and the gap in memory grows
with the class. A recorder that is starved of CPU for seconds splits its files (a stream silent for 2 s ends
its file), as the Go one does; run the benchmark on a machine with nothing else busy.

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
