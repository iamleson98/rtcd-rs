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

/// How a camera is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CameraLayers {
    /// One stream of 640x360 at 30 fps with no temporal layers: what the sim sent before it knew
    /// the browser's encodings, kept so that those runs can be reproduced.
    Single,
    /// Two simulcast layers (`l` and `h`), each with three temporal layers, as lms-web's
    /// `cameraEncodings` sends them.
    #[default]
    Simulcast,
}

impl CameraLayers {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "single" => Ok(Self::Single),
            "simulcast" => Ok(Self::Simulcast),
            _ => Err("--camera-layers is single or simulcast".into()),
        }
    }
}

/// The files, made if they are not there.
#[derive(Debug, Clone)]
pub struct Assets {
    pub camera_layers: CameraLayers,
    /// The single-layer camera.
    pub camera: Clip,
    /// The small simulcast layer, the same for everyone: 320x180, 15 fps, 150 kbit/s.
    pub camera_low: Clip,
    /// The large simulcast layer of the teacher: 640x360, 24 fps, 600 kbit/s.
    pub camera_teacher: Clip,
    /// The large simulcast layer of a student: 640x360, 20 fps, 400 kbit/s.
    pub camera_student: Clip,
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
    /// The same media, with cameras sent as `layers`.
    pub fn with_camera_layers(mut self, layers: CameraLayers) -> Self {
        self.camera_layers = layers;
        self
    }

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
        // One simulcast layer of a camera, as a browser's encoder makes it: VP8 with three temporal
        // layers (frame `n` is in layer 0, 2, 1, 2 for `n` mod 4 = 0, 1, 2, 3) and a keyframe only
        // at the start, for the rest on request.
        let layer = |name: &str, size: &str, fps: u32, kbps: u32| -> std::io::Result<PathBuf> {
            let src = format!("testsrc2=size={size}:rate={fps}");
            let (b, ts) = (
                format!("{kbps}k"),
                format!(
                    "ts_number_layers=3:ts_target_bitrate={},{},{kbps}:ts_rate_decimator=4,2,1:ts_periodicity=4:ts_layer_id=0,2,1,2",
                    kbps * 2 / 5,
                    kbps * 3 / 5,
                ),
            );
            make(
                name,
                &[
                    "-f",
                    "lavfi",
                    "-i",
                    &src,
                    "-t",
                    "20",
                    "-c:v",
                    "libvpx",
                    "-b:v",
                    &b,
                    "-g",
                    "3000",
                    "-deadline",
                    "realtime",
                    "-cpu-used",
                    "8",
                    "-ts-parameters",
                    &ts,
                    "-f",
                    "ivf",
                ],
            )
        };
        let camera_low = layer("cam-l.ivf", "320x180", 15, 150)?;
        let camera_teacher = layer("cam-h-teacher.ivf", "640x360", 24, 600)?;
        let camera_student = layer("cam-h-student.ivf", "640x360", 20, 400)?;
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
            camera_layers: CameraLayers::default(),
            camera: Clip::read(&camera)?,
            camera_low: Clip::read(&camera_low)?,
            camera_teacher: Clip::read(&camera_teacher)?,
            camera_student: Clip::read(&camera_student)?,
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
        for (clip, size, fps) in [
            (&a.camera_low, (320, 180), 15),
            (&a.camera_teacher, (640, 360), 24),
            (&a.camera_student, (640, 360), 20),
        ] {
            assert_eq!((clip.width, clip.height, clip.fps), (size.0, size.1, fps));
            assert!(clip.frames[0].1, "starts on a keyframe");
            assert_eq!(clip.frames.len() % 4, 0, "the temporal pattern loops whole");
            assert_eq!(clip.frames.iter().filter(|f| f.1).count(), 1);
            // Frames of temporal layer 0 are the base the others lean on, so they are the larger.
            let mean = |tid: fn(usize) -> bool| {
                let sizes: Vec<usize> = (1..clip.frames.len())
                    .filter(|n| tid(*n))
                    .map(|n| clip.frames[n].0.len())
                    .collect();
                sizes.iter().sum::<usize>() / sizes.len()
            };
            assert!(
                mean(|n| n % 4 == 0) > mean(|n| n % 2 == 1),
                "frames 0 mod 4 are layer 0 and the odd ones layer 2"
            );
        }
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
