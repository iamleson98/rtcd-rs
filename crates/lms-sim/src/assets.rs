//! The media the synthetic participants send: real VP8 frames (an IVF file) and real Opus packets
//! (an Ogg file), made with ffmpeg the first time and replayed in a loop.

use std::path::{Path, PathBuf};
use std::process::Command;

/// One clip of VP8 frames.
#[derive(Debug, Clone)]
pub struct Clip {
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    /// Each frame's bytes and whether it is a keyframe.
    pub frames: Vec<(Vec<u8>, bool)>,
}

impl Clip {
    /// Reads an IVF file.
    pub fn read(path: &Path) -> std::io::Result<Self> {
        let data = std::fs::read(path)?;
        let bad = || std::io::Error::other(format!("{} is not an IVF file", path.display()));
        if data.len() < 32 || &data[..4] != b"DKIF" {
            return Err(bad());
        }
        let width = u32::from(u16::from_le_bytes([data[12], data[13]]));
        let height = u32::from(u16::from_le_bytes([data[14], data[15]]));
        let den = u32::from_le_bytes(data[16..20].try_into().unwrap());
        let num = u32::from_le_bytes(data[20..24].try_into().unwrap()).max(1);
        let mut frames = Vec::new();
        let mut at = 32;
        while at + 12 <= data.len() {
            let n = u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as usize;
            at += 12;
            if at + n > data.len() {
                break;
            }
            let f = data[at..at + n].to_vec();
            let key = f.first().is_some_and(|b| b & 1 == 0);
            frames.push((f, key));
            at += n;
        }
        if frames.is_empty() || !frames[0].1 {
            return Err(bad());
        }
        Ok(Self {
            width,
            height,
            fps: (den / num).max(1),
            frames,
        })
    }
}

/// Opus packets, 20 ms each.
#[derive(Debug, Clone)]
pub struct Voice {
    pub packets: Vec<Vec<u8>>,
}

impl Voice {
    /// Reads an Ogg Opus file: every packet after the two header packets.
    pub fn read(path: &Path) -> std::io::Result<Self> {
        let data = std::fs::read(path)?;
        let bad = || std::io::Error::other(format!("{} is not an Ogg Opus file", path.display()));
        let mut packets = Vec::new();
        let mut partial = Vec::new();
        let mut at = 0;
        let mut headers = 0;
        while at + 27 <= data.len() {
            if &data[at..at + 4] != b"OggS" {
                return Err(bad());
            }
            let segments = data[at + 26] as usize;
            let table = data.get(at + 27..at + 27 + segments).ok_or_else(bad)?;
            let mut body = at + 27 + segments;
            for &lace in table {
                let end = body + lace as usize;
                partial.extend_from_slice(data.get(body..end).ok_or_else(bad)?);
                body = end;
                if lace < 255 {
                    if headers < 2 {
                        headers += 1;
                    } else if !partial.is_empty() {
                        packets.push(std::mem::take(&mut partial));
                    }
                    partial.clear();
                }
            }
            at = body;
        }
        if packets.is_empty() {
            return Err(bad());
        }
        Ok(Self { packets })
    }
}

/// The files, made if they are not there.
#[derive(Debug, Clone)]
pub struct Assets {
    pub camera: Clip,
    pub screen: Clip,
    pub voice: Voice,
}

fn ffmpeg(args: &[&str]) -> std::io::Result<()> {
    let out = Command::new("ffmpeg")
        .args(["-hide_banner", "-loglevel", "error", "-y"])
        .args(args)
        .output()?;
    if out.status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ))
    }
}

impl Assets {
    pub fn load(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let make = |name: &str, args: &[&str]| -> std::io::Result<PathBuf> {
            let path = dir.join(name);
            if !path.exists() {
                let tmp = dir.join(format!("{name}.tmp"));
                let mut a = args.to_vec();
                let t = tmp.to_str().unwrap().to_owned();
                a.push(&t);
                ffmpeg(&a)?;
                std::fs::rename(&tmp, &path)?;
            }
            Ok(path)
        };
        // A camera: 640x360 at 30 fps, 500 kb/s, a keyframe every two seconds.
        let camera = make(
            "camera.ivf",
            &[
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=640x360:rate=30",
                "-t",
                "20",
                "-c:v",
                "libvpx",
                "-b:v",
                "500k",
                "-g",
                "60",
                "-deadline",
                "realtime",
                "-cpu-used",
                "8",
                "-f",
                "ivf",
            ],
        )?;
        // A screen: 1280x720 at 15 fps, mostly still, a keyframe every four seconds.
        let screen = make(
            "screen.ivf",
            &[
                "-f",
                "lavfi",
                "-i",
                "smptebars=size=1280x720:rate=15",
                "-t",
                "20",
                "-c:v",
                "libvpx",
                "-b:v",
                "1500k",
                "-g",
                "60",
                "-deadline",
                "realtime",
                "-cpu-used",
                "8",
                "-f",
                "ivf",
            ],
        )?;
        let voice = make(
            "voice.opus",
            &[
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=330:sample_rate=48000",
                "-t",
                "20",
                "-c:a",
                "libopus",
                "-b:a",
                "32k",
                "-frame_duration",
                "20",
                "-f",
                "ogg",
            ],
        )?;
        Ok(Self {
            camera: Clip::read(&camera)?,
            screen: Clip::read(&screen)?,
            voice: Voice::read(&voice)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_clips_are_real_media() {
        if Command::new("ffmpeg").arg("-version").output().is_err() {
            return;
        }
        let dir = std::env::temp_dir().join(format!("lms-sim-assets-{}", std::process::id()));
        let a = Assets::load(&dir).unwrap();
        assert_eq!(
            (a.camera.width, a.camera.height, a.camera.fps),
            (640, 360, 30)
        );
        assert!(a.camera.frames.len() >= 590 && a.camera.frames[0].1);
        assert!(
            a.camera.frames.iter().filter(|f| f.1).count() >= 9,
            "keyframes every two seconds"
        );
        assert_eq!((a.screen.width, a.screen.height), (1280, 720));
        assert!(
            a.voice.packets.len() >= 990,
            "{} packets",
            a.voice.packets.len()
        );
        assert!(a.voice.packets.iter().all(|p| p.len() < 400));
        let _ = std::fs::remove_dir_all(dir);
    }
}
