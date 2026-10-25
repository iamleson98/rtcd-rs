#!/usr/bin/env python3
"""Counts the frames of a recorded video that match no frame of the clip it was made from.

    scripts/frame-damage.py CLIP.ivf RECORDING.webm...

Every frame is reduced to 96x54 grey; a recorded frame whose nearest original frame is further away
than a threshold is a damaged picture (what a decoder shows when a frame it refers to was lost).
Needs ffmpeg and numpy.
"""
import subprocess
import sys

import numpy as np

W, H = 96, 54
# A frame this far (mean squared error, grey levels) from every original is not one of them: the
# decoder's own rounding between two encodes of a frame is far below it.
THRESHOLD = 1.0


def frames(path):
    out = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", path, "-vf", f"scale={W}:{H}:flags=area,format=gray", "-f", "rawvideo", "-"],
        capture_output=True, check=True,
    ).stdout
    return np.frombuffer(out, dtype=np.uint8).reshape(-1, W * H).astype(np.float32)


def main():
    clip = frames(sys.argv[1])
    for rec in sys.argv[2:]:
        got = frames(rec)
        # Mean squared error of each recorded frame against each original, nearest one.
        d = np.empty(len(got), dtype=np.float32)
        for i, g in enumerate(got):
            d[i] = ((clip - g) ** 2).mean(axis=1).min()
        damaged = int((d > THRESHOLD).sum())
        print(f"{rec}: {len(got)} frames, {damaged} damaged ({100 * damaged / max(1, len(got)):.1f}%), worst MSE {d.max():.0f}")


if __name__ == "__main__":
    main()
