//! Each file of a recording is uploaded as an object of its own, numbered by its place in the
//! manifest. The server says where each one goes: with direct access, straight to storage at URLs
//! it presigns (the bytes leave the cluster once, and never pass through the server), otherwise to
//! the server. Asking for those URLs a batch at a time also tells the server the upload is still
//! going.

use std::io::SeekFrom;
use std::path::Path;
use std::time::Duration;

use futures_util::TryStreamExt;
use futures_util::stream::{self, StreamExt};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Method};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use tracing::{info, warn};

use crate::api::{Api, ApiError};
use crate::media::Entry;
use crate::throttle::Throttle;

/// The most files the server takes in one request for URLs; the byte limit keeps a batch short
/// enough that the next request comes well within the server's patience.
const BATCH_FILES: usize = 1000;
const BATCH_BYTES: u64 = 256 << 20;
/// The slowest an upload may go before it is cut off (and retried): far below the cluster's capped
/// 10 Mb/s.
const UPLOAD_RATE: u64 = 256 << 10;

/// How long a request of `size` bytes may take. Held to a node-wide rate shared by every request in
/// flight, a request gets a share of it, and it may be held to a share of that (a quarter while the
/// node's own media needs the network) that a request started at the full rate finds cut under it:
/// the allowance is for the slowest of the two, the quarter or the current share, divided among the
/// requests in flight, and twice what it takes (the number in flight may grow while it is sent).
fn request_timeout(size: u64, throttle: Option<&Throttle>) -> Duration {
    let rate = throttle.map_or(UPLOAD_RATE as f64, |t| {
        (UPLOAD_RATE as f64).min(t.fair_rate(t.share().min(250)) / 2.0)
    });
    Duration::from_secs(60 + (size as f64 / rate) as u64)
}

/// How many files, and how many parts of one file, are in flight at once. One connection to
/// storage carries a few MB/s, far below what the node's link does, and the recording is not
/// watchable before the last byte is in.
const FILES_AT_ONCE: usize = 4;
const PARTS_AT_ONCE: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("the recording is empty")]
    Empty,
    #[error("{0}")]
    Protocol(String),
    #[error("file {index}: {source}")]
    File {
        index: usize,
        source: Box<UploadError>,
    },
    #[error("part {part} of {total}: {source}")]
    Part {
        part: usize,
        total: usize,
        source: Box<UploadError>,
    },
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// Orders the files along the timeline (their order in the manifest, and so their numbers) and
/// fills in their sizes.
pub fn manifest_files(mut files: Vec<Entry>) -> std::io::Result<Vec<Entry>> {
    files.sort_by_key(|f| f.start_ms);
    for f in &mut files {
        f.size = std::fs::metadata(&f.path)?.len();
    }
    Ok(files)
}

#[derive(Serialize)]
struct UploadRequest<'a> {
    index: usize,
    kind: &'a str,
    size: u64,
    #[serde(skip_serializing_if = "str::is_empty")]
    upload_id: &'a str,
    #[serde(skip_serializing_if = "is_zero")]
    first_part: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Part {
    pub number: usize,
    pub url: String,
}

/// Where one file goes: `url` (the whole file), or `upload_id` and the URLs of its next parts,
/// each `part_size` long but the last.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Target {
    #[serde(default)]
    pub index: usize,
    #[serde(default)]
    pub content_type: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub upload_id: String,
    #[serde(default)]
    pub part_size: u64,
    #[serde(default)]
    pub parts: Vec<Part>,
}

#[derive(Debug, Deserialize)]
struct Targets {
    #[serde(default)]
    direct: bool,
    #[serde(default)]
    uploads: Vec<Target>,
}

/// A file uploaded in parts, for the server to finish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MultipartDone {
    pub index: usize,
    pub upload_id: String,
}

/// A presigned URL storage no longer takes.
fn expired(e: &UploadError) -> bool {
    matches!(e, UploadError::Api(a) if a.status() == Some(403))
}

impl Api {
    /// Sends every file and returns the uploads in parts to finish. The biggest go first, so the
    /// last to finish is never a big one started late.
    pub async fn upload(&self, files: &[Entry]) -> Result<Vec<MultipartDone>, UploadError> {
        if files.is_empty() {
            return Err(UploadError::Empty);
        }
        let mut multipart = Vec::new();
        let mut start = 0;
        while start < files.len() {
            let (mut end, mut size) = (start, 0u64);
            while end < files.len()
                && end - start < BATCH_FILES
                && (end == start || size + files[end].size <= BATCH_BYTES)
            {
                size += files[end].size;
                end += 1;
            }
            let batch: Vec<_> = (start..end)
                .map(|n| UploadRequest {
                    index: n,
                    kind: &files[n].kind,
                    size: files[n].size,
                    upload_id: "",
                    first_part: 0,
                })
                .collect();
            let targets = self.targets(&batch).await?;
            let done = if !targets.direct {
                let mut order: Vec<usize> = (start..end).collect();
                order.sort_by_key(|&n| std::cmp::Reverse(files[n].size));
                self.in_parallel(order.into_iter().map(|n| async move {
                    self.upload_to_server(n, &files[n])
                        .await
                        .map_err(|e| UploadError::File {
                            index: n,
                            source: Box::new(e),
                        })?;
                    Ok(None)
                }))
                .await?
            } else {
                if targets.uploads.len() != batch.len() {
                    return Err(UploadError::Protocol(format!(
                        "asked where {} files go, told {}",
                        batch.len(),
                        targets.uploads.len()
                    )));
                }
                let mut uploads = targets.uploads;
                for t in &uploads {
                    if t.index < start || t.index >= end {
                        return Err(UploadError::Protocol(format!(
                            "told where file {} goes, not asked",
                            t.index
                        )));
                    }
                }
                uploads.sort_by_key(|t| std::cmp::Reverse(files[t.index].size));
                self.in_parallel(uploads.into_iter().map(|t| async move {
                    let n = t.index;
                    self.send_direct(n, &files[n], t)
                        .await
                        .map_err(|e| UploadError::File {
                            index: n,
                            source: Box::new(e),
                        })
                }))
                .await?
            };
            multipart.extend(done.into_iter().flatten());
            info!(files = end, of = files.len(), "uploaded files");
            start = end;
        }
        multipart.sort_by_key(|m| m.index);
        Ok(multipart)
    }

    /// Sends one file as object `index`, wherever the server says it goes, and returns the upload
    /// in parts for the server to finish, if it was one. For a recording whose files are uploaded
    /// as they close, so their numbers are not their places in a list.
    pub async fn upload_file(
        &self,
        index: usize,
        file: &Entry,
    ) -> Result<Option<MultipartDone>, UploadError> {
        let ask = [UploadRequest {
            index,
            kind: &file.kind,
            size: file.size,
            upload_id: "",
            first_part: 0,
        }];
        let mut targets = self.targets(&ask).await?;
        let sent = if !targets.direct {
            self.upload_to_server(index, file).await.map(|()| None)
        } else {
            let t = match (targets.uploads.pop(), targets.uploads.is_empty()) {
                (Some(t), true) if t.index == index => t,
                _ => {
                    return Err(UploadError::Protocol(format!(
                        "asked where file {index} goes, told something else"
                    )));
                }
            };
            self.send_direct(index, file, t).await
        };
        sent.map_err(|e| UploadError::File {
            index,
            source: Box::new(e),
        })
    }

    /// Sends a file to the storage URLs `t` names: whole, or in parts.
    async fn send_direct(
        &self,
        n: usize,
        f: &Entry,
        t: Target,
    ) -> Result<Option<MultipartDone>, UploadError> {
        if t.upload_id.is_empty() {
            self.upload_whole(n, f, t).await.map(|()| None)
        } else {
            let upload_id = t.upload_id.clone();
            self.upload_parts(n, f, t).await.map(|()| {
                Some(MultipartDone {
                    index: n,
                    upload_id,
                })
            })
        }
    }

    /// Runs the uploads a few at a time; the first failure ends them all.
    async fn in_parallel<F>(
        &self,
        jobs: impl Iterator<Item = F>,
    ) -> Result<Vec<Option<MultipartDone>>, UploadError>
    where
        F: Future<Output = Result<Option<MultipartDone>, UploadError>>,
    {
        stream::iter(jobs)
            .buffer_unordered(FILES_AT_ONCE)
            .try_collect()
            .await
    }

    async fn targets(&self, files: &[UploadRequest<'_>]) -> Result<Targets, UploadError> {
        let body = serde_json::to_vec(&serde_json::json!({ "files": files }))
            .map_err(|e| UploadError::Protocol(e.to_string()))?;
        Ok(self
            .retry("upload URLs", || async {
                let req = self
                    .authed(Method::POST, &format!("{}/uploads", self.base))
                    .header(CONTENT_TYPE, "application/json")
                    .body(body.clone());
                Api::json::<Targets>(req).await
            })
            .await?)
    }

    /// Asks again where one file goes (its URLs ran out, or for its next parts).
    async fn target(&self, req: UploadRequest<'_>) -> Result<Target, UploadError> {
        let index = req.index;
        let mut out = self.targets(&[req]).await?;
        if !out.direct || out.uploads.len() != 1 || out.uploads[0].index != index {
            return Err(UploadError::Protocol(
                "storage is no longer directly reachable".into(),
            ));
        }
        Ok(out.uploads.remove(0))
    }

    async fn upload_whole(&self, n: usize, f: &Entry, t: Target) -> Result<(), UploadError> {
        let result = self
            .put(&t.url, Some(&t.content_type), None, &f.path, 0, f.size)
            .await;
        match result {
            Err(e) if expired(&e) => {
                let t = self
                    .target(UploadRequest {
                        index: n,
                        kind: &f.kind,
                        size: f.size,
                        upload_id: "",
                        first_part: 0,
                    })
                    .await?;
                self.put(&t.url, Some(&t.content_type), None, &f.path, 0, f.size)
                    .await
            }
            other => other,
        }
    }

    /// Sends a file's parts a few at a time, asking for the next parts' URLs as it goes (and once
    /// more for a part whose URL ran out).
    async fn upload_parts(&self, n: usize, f: &Entry, mut t: Target) -> Result<(), UploadError> {
        if t.part_size == 0 {
            return Err(UploadError::Protocol("no part size".into()));
        }
        let part_size = t.part_size;
        let total = f.size.div_ceil(part_size) as usize;
        let mut pending: Vec<usize> = (1..=total).collect();
        let mut reminted = Vec::new();
        while let Some(&first) = pending.first() {
            if t.parts.first().is_none_or(|p| p.number != first) {
                t = self
                    .target(UploadRequest {
                        index: n,
                        kind: &f.kind,
                        size: f.size,
                        upload_id: &t.upload_id,
                        first_part: first,
                    })
                    .await?;
                if t.parts.first().is_none_or(|p| p.number != first) {
                    return Err(UploadError::Protocol(format!("no URL for part {first}")));
                }
            }
            let urls: Vec<Part> = std::mem::take(&mut t.parts)
                .into_iter()
                .filter(|p| pending.contains(&p.number))
                .collect();
            let results: Vec<(usize, Result<(), UploadError>)> =
                stream::iter(urls.into_iter().map(|p| async move {
                    let offset = (p.number as u64 - 1) * part_size;
                    let len = part_size.min(f.size - offset);
                    (
                        p.number,
                        self.put(&p.url, None, None, &f.path, offset, len).await,
                    )
                }))
                .buffer_unordered(PARTS_AT_ONCE)
                .collect()
                .await;
            for (number, result) in results {
                match result {
                    Ok(()) => pending.retain(|&p| p != number),
                    // The part stays to send, with a fresh URL (once: a URL that keeps running
                    // out is an error).
                    Err(e) if expired(&e) && !reminted.contains(&number) => reminted.push(number),
                    Err(e) => {
                        return Err(UploadError::Part {
                            part: number,
                            total,
                            source: Box::new(e),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    async fn upload_to_server(&self, n: usize, f: &Entry) -> Result<(), UploadError> {
        let url = format!("{}/files/{n}", self.base);
        self.put(
            &url,
            Some("application/octet-stream"),
            Some(&self.token),
            &f.path,
            0,
            f.size,
        )
        .await
    }

    /// Sends `size` bytes of the file at `path` from `offset`, retried on network and server
    /// errors (a PUT replaces what an earlier attempt left).
    async fn put(
        &self,
        url: &str,
        content_type: Option<&str>,
        bearer: Option<&str>,
        path: &Path,
        offset: u64,
        size: u64,
    ) -> Result<(), UploadError> {
        Ok(self
            .retry("upload", || async {
                let _flight = self.throttle.as_ref().map(Throttle::enter);
                let mut file = tokio::fs::File::open(path)
                    .await
                    .map_err(|e| ApiError::Network(e.to_string()))?;
                file.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(|e| ApiError::Network(e.to_string()))?;
                let throttle = self.throttle.clone();
                let stream = ReaderStream::with_capacity(file.take(size), 256 << 10)
                    .then(move |chunk| {
                        let throttle = throttle.clone();
                        async move {
                            if let (Ok(bytes), Some(t)) = (&chunk, &throttle) {
                                t.take(bytes.len() as u64).await;
                            }
                            chunk
                        }
                    })
                    .map_err(std::io::Error::other);
                let mut req = self
                    .uploads
                    .put(url)
                    .timeout(request_timeout(size, self.throttle.as_deref()))
                    .header(CONTENT_LENGTH, size)
                    .body(Body::wrap_stream(stream));
                if let Some(ct) = content_type {
                    req = req.header(CONTENT_TYPE, ct);
                }
                if let Some(b) = bearer {
                    req = req.bearer_auth(b);
                }
                Api::send(req).await.map(|_| ())
            })
            .await?)
    }

    /// Runs `f` until it succeeds, fails for good (a 4xx answer) or runs out of attempts, waiting
    /// longer after each failure.
    async fn retry<T, F, Fut>(&self, what: &str, mut f: F) -> Result<T, ApiError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ApiError>>,
    {
        let mut attempt = 0;
        loop {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) if e.retryable() && attempt < self.retries => {
                    attempt += 1;
                    let wait = self.backoff * attempt;
                    warn!(what, wait_ms = wait.as_millis() as u64, error = %e, "retrying");
                    tokio::time::sleep(wait).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_request_held_to_a_share_of_the_rate_is_given_the_time_the_share_takes() {
        use super::*;
        let size: u64 = 16 << 20;
        assert_eq!(request_timeout(size, None).as_secs(), 60 + 64);
        // A node-wide rate of 1 MiB/s: started at the full rate, a request may find the throttle
        // cut to a quarter (256 KiB/s) under it, and wants twice the time that takes.
        let throttle = std::sync::Arc::new(Throttle::new(1 << 20));
        assert_eq!(request_timeout(size, Some(&throttle)).as_secs(), 60 + 128);
        // Eight requests share it: 32 KiB/s each at the quarter, twice the time for 16 MiB.
        let flights: Vec<_> = (0..8).map(|_| throttle.enter()).collect();
        assert_eq!(request_timeout(size, Some(&throttle)).as_secs(), 60 + 1024);
        drop(flights);
        assert_eq!(request_timeout(size, Some(&throttle)).as_secs(), 60 + 128);
        // Held to a tenth: slower than the quarter it was allowed for.
        throttle.set_share(100);
        assert_eq!(request_timeout(size, Some(&throttle)).as_secs(), 60 + 320);
        // A fast node-wide rate never allows less than the slowest the upload may go.
        let fast = Throttle::new(1 << 30);
        assert_eq!(request_timeout(size, Some(&fast)).as_secs(), 60 + 64);
    }

    use std::path::PathBuf;

    use serde_json::{Value, json};

    use super::*;
    use crate::media::{CAMERA, VOICE};
    use crate::testing::{Fake, REC_PATH};

    fn files(tag: &str) -> (Vec<Entry>, [Vec<u8>; 2]) {
        let dir = std::env::temp_dir().join(format!("upload-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let bodies = [
            b"0123456789".to_vec(),
            b"abcdefghijklmnopqrstuvwxy".to_vec(),
        ];
        let mut entries = Vec::new();
        for (i, (kind, start)) in [(VOICE, 100), (CAMERA, 200)].into_iter().enumerate() {
            let path: PathBuf = dir.join(kind);
            std::fs::write(&path, &bodies[i]).unwrap();
            entries.push(Entry {
                kind: kind.into(),
                session_id: String::new(),
                user_id: String::new(),
                start_ms: start,
                end_ms: start + 1,
                width: 0,
                height: 0,
                size: 0,
                path,
            });
        }
        (manifest_files(entries).unwrap(), bodies)
    }

    #[tokio::test]
    async fn files_go_straight_to_storage_in_parts_when_the_server_says_so() {
        let (files, bodies) = files("direct");
        let fake = Fake::start(|r, attempt, site| {
            match r.path.as_str() {
                p if p == format!("{REC_PATH}/uploads") => {
                    let v: Value = serde_json::from_slice(&r.body).unwrap();
                    let asked = v["files"].as_array().unwrap();
                    let part = |n: u64| json!({"number": n, "url": format!("{site}/store/up/{n}")});
                    let mut camera = json!({"index": 1, "content_type": "video/webm", "upload_id": "up", "part_size": 10});
                    if asked.len() == 2 {
                        camera["parts"] = json!([part(1), part(2)]);
                        return (200, Some(json!({"direct": true, "uploads": [
                            {"index": 0, "content_type": "audio/webm", "url": format!("{site}/store/voice")},
                            camera
                        ]})));
                    }
                    if asked[0]["index"] == 0 {
                        return (200, Some(json!({"direct": true, "uploads": [
                            {"index": 0, "content_type": "audio/webm", "url": format!("{site}/store/voice-again")}
                        ]})));
                    }
                    camera["parts"] = json!([part(asked[0]["first_part"].as_u64().unwrap())]);
                    (200, Some(json!({"direct": true, "uploads": [camera]})))
                }
                // Its URL ran out: the recorder asks for a fresh one.
                "/store/voice" => (403, None),
                "/store/up/2" if attempt == 1 => (502, None),
                _ => (200, None),
            }
        })
        .await;
        let multipart = fake.api().upload(&files).await.unwrap();
        assert_eq!(
            multipart,
            [MultipartDone {
                index: 1,
                upload_id: "up".into()
            }]
        );

        let seen = fake.seen();
        // Files and parts go at the same time: what was sent, not in which order.
        let mut calls: Vec<String> = seen
            .iter()
            .map(|r| {
                format!(
                    "{} {}",
                    r.method,
                    r.path.strip_prefix(REC_PATH).unwrap_or(&r.path)
                )
            })
            .collect();
        calls.sort();
        assert_eq!(
            calls,
            [
                "POST /uploads",
                "POST /uploads",
                "POST /uploads",
                "PUT /store/up/1",
                "PUT /store/up/2",
                "PUT /store/up/2",
                "PUT /store/up/3",
                "PUT /store/voice",
                "PUT /store/voice-again",
            ]
        );
        for r in &seen {
            if r.path.starts_with("/store/") {
                assert_eq!(r.auth, "", "the bot's token went to storage: {}", r.path);
            }
            if r.path == "/store/voice-again" {
                assert_eq!(r.body, bodies[0]);
                assert_eq!(r.content_type, "audio/webm");
            }
        }
        let camera: Vec<u8> = ["/store/up/1", "/store/up/2", "/store/up/3"]
            .iter()
            .flat_map(|p| {
                seen.iter()
                    .rev()
                    .find(|r| r.path == *p && r.method == "PUT")
                    .unwrap()
                    .body
                    .clone()
            })
            .collect();
        assert_eq!(camera, bodies[1]);
    }

    #[tokio::test]
    async fn files_go_through_the_server_when_storage_is_not_reachable() {
        let (files, bodies) = files("server");
        let fake = Fake::start(|r, attempt, _| {
            if r.path.ends_with("/uploads") {
                (200, Some(json!({"direct": false, "uploads": []})))
            } else if r.path.ends_with("/files/1") && attempt == 1 {
                (502, None)
            } else {
                (200, None)
            }
        })
        .await;
        let api = fake.api();
        let multipart = api.upload(&files).await.unwrap();
        api.complete(Duration::from_millis(93_500), &files, &multipart)
            .await
            .unwrap();

        let seen = fake.seen();
        let mut calls: Vec<String> = seen
            .iter()
            .map(|r| format!("{} {}", r.method, r.path.strip_prefix(REC_PATH).unwrap()))
            .collect();
        // The two files go at the same time, the biggest started first; the completion comes last.
        assert_eq!(calls.pop().as_deref(), Some("POST /complete"));
        assert_eq!(calls.remove(0), "POST /uploads");
        calls.sort();
        assert_eq!(calls, ["PUT /files/0", "PUT /files/1", "PUT /files/1"]);
        assert!(seen.iter().all(|r| r.auth == "Bearer bot-token"));
        let sent = |path: &str| -> Vec<Vec<u8>> {
            seen.iter()
                .filter(|r| r.method == "PUT" && r.path == format!("{REC_PATH}{path}"))
                .map(|r| r.body.clone())
                .collect()
        };
        assert_eq!(sent("/files/0"), [bodies[0].clone()]);
        assert_eq!(
            sent("/files/1"),
            [bodies[1].clone(), bodies[1].clone()],
            "the file is sent whole again after a failure"
        );
        let body: Value = serde_json::from_slice(&seen.last().unwrap().body).unwrap();
        assert_eq!(body["duration_ms"], 93_500);
        assert_eq!(body["files"].as_array().unwrap().len(), 2);
        assert_eq!(body["files"][1]["size"], 25);
        assert_eq!(body["files"][0]["kind"], "voice");
        assert_eq!(body["multipart"], json!([]));
    }

    #[tokio::test]
    async fn one_file_is_sent_as_the_object_its_index_names() {
        let (files, bodies) = files("one");
        // Through the server.
        let fake = Fake::start(|r, _, _| {
            if r.path.ends_with("/uploads") {
                (200, Some(json!({"direct": false, "uploads": []})))
            } else {
                (200, None)
            }
        })
        .await;
        let done = fake.api().upload_file(1007, &files[1]).await.unwrap();
        assert_eq!(done, None);
        let seen = fake.seen();
        let asked: Value = serde_json::from_slice(&seen[0].body).unwrap();
        assert_eq!(
            asked["files"],
            json!([{"index": 1007, "kind": "camera", "size": 25}])
        );
        assert_eq!(seen[1].path, format!("{REC_PATH}/files/1007"));
        assert_eq!(seen[1].body, bodies[1]);
        assert_eq!(seen[1].auth, "Bearer bot-token");

        // Straight to storage, whole and in parts: the answer names the file asked for.
        let fake = Fake::start(|r, _, site| {
            if r.path.ends_with("/uploads") {
                let v: Value = serde_json::from_slice(&r.body).unwrap();
                let kind = v["files"][0]["kind"].as_str().unwrap();
                if kind == "voice" {
                    (200, Some(json!({"direct": true, "uploads": [
                        {"index": 1008, "content_type": "audio/webm", "url": format!("{site}/store/voice")}
                    ]})))
                } else {
                    let part = |n: u64| json!({"number": n, "url": format!("{site}/store/up/{n}")});
                    (200, Some(json!({"direct": true, "uploads": [
                        {"index": 1009, "content_type": "video/webm", "upload_id": "up", "part_size": 10, "parts": [part(1), part(2), part(3)]}
                    ]})))
                }
            } else {
                (200, None)
            }
        })
        .await;
        let api = fake.api();
        assert_eq!(api.upload_file(1008, &files[0]).await.unwrap(), None);
        assert_eq!(
            api.upload_file(1009, &files[1]).await.unwrap(),
            Some(MultipartDone {
                index: 1009,
                upload_id: "up".into()
            })
        );
        let seen = fake.seen();
        assert!(
            seen.iter()
                .filter(|r| r.path.starts_with("/store/"))
                .all(|r| r.auth.is_empty()),
            "the bot's token never goes to storage"
        );
        let stored: Vec<u8> = ["/store/up/1", "/store/up/2", "/store/up/3"]
            .iter()
            .flat_map(|p| seen.iter().find(|r| r.path == *p).unwrap().body.clone())
            .collect();
        assert_eq!(stored, bodies[1]);

        // An answer about another file is a protocol error, not a wrong upload.
        let fake = Fake::start(|r, _, site| {
            if r.path.ends_with("/uploads") {
                (
                    200,
                    Some(json!({"direct": true, "uploads": [
                        {"index": 5, "content_type": "audio/webm", "url": format!("{site}/store/x")}
                    ]})),
                )
            } else {
                (200, None)
            }
        })
        .await;
        let err = fake.api().upload_file(1007, &files[0]).await.unwrap_err();
        assert!(matches!(err, UploadError::Protocol(_)), "{err}");
        assert_eq!(fake.seen().len(), 1, "nothing was sent to the wrong place");
    }

    #[tokio::test]
    async fn uploads_are_held_to_the_rate_of_their_bucket() {
        let dir = std::env::temp_dir().join(format!("upload-{}-throttle", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("big");
        std::fs::write(&path, vec![7u8; 600_000]).unwrap();
        let entry = manifest_files(vec![Entry {
            kind: VOICE.into(),
            session_id: String::new(),
            user_id: String::new(),
            start_ms: 0,
            end_ms: 1,
            width: 0,
            height: 0,
            size: 0,
            path,
        }])
        .unwrap();
        let fake = Fake::start(|r, _, _| {
            if r.path.ends_with("/uploads") {
                (200, Some(json!({"direct": false, "uploads": []})))
            } else {
                (200, None)
            }
        })
        .await;
        // 600 kB at 1 MB/s with a burst of 250 kB: about 350 ms past the burst.
        let api = fake
            .api()
            .throttled(std::sync::Arc::new(crate::throttle::Throttle::new(
                1_000_000,
            )));
        let started = std::time::Instant::now();
        api.upload_file(0, &entry[0]).await.unwrap();
        let took = started.elapsed();
        assert!(took >= Duration::from_millis(300), "{took:?}");
        assert_eq!(fake.seen().last().unwrap().body.len(), 600_000);
    }

    #[tokio::test]
    async fn a_refusal_ends_the_upload_and_an_empty_recording_is_an_error() {
        let (files, _) = files("refused");
        let fake = Fake::start(|_, _, _| (409, None)).await;
        assert!(fake.api().upload(&files).await.is_err());
        assert_eq!(fake.seen().len(), 1, "a 4xx is not retried");
        assert!(matches!(
            fake.api().upload(&[]).await,
            Err(UploadError::Empty)
        ));
    }

    #[tokio::test]
    async fn a_server_that_keeps_failing_is_given_up_on() {
        let (files, _) = files("failing");
        let fake = Fake::start(|_, _, _| (503, None)).await;
        let mut api = fake.api();
        api.retries = 3;
        assert!(api.upload(&files).await.is_err());
        assert_eq!(fake.seen().len(), 4, "the first try and three retries");
    }

    #[test]
    fn the_manifest_is_in_timeline_order_with_sizes() {
        let dir = std::env::temp_dir().join(format!("upload-{}-manifest", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let write = |name: &str, body: &str| {
            let p = dir.join(name);
            std::fs::write(&p, body).unwrap();
            p
        };
        let entry = |kind: &str, start: i64, path: PathBuf| Entry {
            kind: kind.into(),
            session_id: String::new(),
            user_id: String::new(),
            start_ms: start,
            end_ms: 0,
            width: 0,
            height: 0,
            size: 0,
            path,
        };
        let got = manifest_files(vec![
            entry("screen", 0, write("screen", "SCREEN")),
            entry("camera", 500, write("cam", "CAMERA")),
            entry("voice", 100, write("voice", "VO")),
        ])
        .unwrap();
        let got: Vec<String> = got
            .iter()
            .map(|f| format!("{}:{}", f.kind, f.size))
            .collect();
        assert_eq!(got, ["screen:6", "voice:2", "camera:6"]);
        assert!(manifest_files(vec![entry("screen", 0, dir.join("gone"))]).is_err());
    }
}
