#!/usr/bin/env python3
"""Reads a calibration ramp: how many classes the SFU carried before it collapsed.

    ceiling.py --proc proc.jsonl --recv clean.jsonl --ramp ramp.jsonl --per-step N
               [--cpu-sat 180] [--sat-secs 30] [--freeze-max 6] [--write FILE --impl rs --arch x86_64
               --students 30 --max-classes 40]

Prints one JSON line: {"reason": null | "cpu" | "quality", "at": <unix ms>, "classes_at": N,
"ceiling_classes": N, "cpu_peak": P}. It is safe to call while the ramp runs (the files grow). The
SFU has collapsed when

* cpu: its `proc` samples are at or above `--cpu-sat` % of a core (180 of the 200 of two cores) for
  `--sat-secs` seconds in a row; or
* quality: the measured receivers (video `recv` rows) freeze `--freeze-max` times a minute or more
  per track over 60 s, or at least half of the receivers' tracks report `stalled` in three samples
  in a row.

`ceiling_classes` is the number of classes present when the collapse began, less the classes of the
step that began it (what the server carried healthily); a ramp that ended without a collapse
(`reason` null; written as `max`) has the classes of its last step. With `--write` the result is
written as the ceiling file `hold.py` reads.
"""
import argparse
import json
import sys
from bisect import bisect_right
from collections import defaultdict


def rows_of(path):
    rows = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except OSError:
        pass
    return rows


def cpu_collapse(proc, cpu_sat, sat_secs):
    """(start ts in ms of the first saturated stretch of sat_secs or more, or None; peak cpu %)."""
    samples = sorted((r["ts"], r["cpu_pct"]) for r in proc if r.get("kind") == "proc" and "cpu_pct" in r and "ts" in r)
    peak = max((c for _, c in samples), default=0.0)
    start = None
    for ts, cpu in samples:
        if cpu >= cpu_sat:
            if start is None:
                start = ts
            if ts - start >= sat_secs * 1000:
                return start, peak
        else:
            start = None
    return None, peak


def quality_collapse(recv, freeze_max, window_s=60, stalled_samples=3):
    """Start ts in ms of the first window in which the receivers collapsed, or None."""
    tracks = defaultdict(list)
    for r in recv:
        if r.get("kind") == "recv" and "ts" in r:
            tracks[(r.get("id"), r.get("media"))].append(r)
    for series in tracks.values():
        series.sort(key=lambda r: r["ts"])
    window = window_s * 1000
    video = [s for (_, media), s in tracks.items() if media == "video"]
    times = sorted({r["ts"] for s in tracks.values() for r in s})
    for t in times:
        if len(video) and t - times[0] >= window:
            per_min = []
            for s in video:
                stamps = [r["ts"] for r in s]
                hi = bisect_right(stamps, t) - 1
                lo = bisect_right(stamps, t - window) - 1
                if lo < 0 or hi <= lo or stamps[hi] - stamps[lo] < window * 0.75:
                    continue
                span_min = (stamps[hi] - stamps[lo]) / 60000.0
                per_min.append((s[hi].get("freezes", 0) - s[lo].get("freezes", 0)) / span_min)
            if per_min and sum(per_min) / len(per_min) >= freeze_max:
                return t - window
        stalled = 0
        for s in tracks.values():
            stamps = [r["ts"] for r in s]
            hi = bisect_right(stamps, t)
            last = s[max(0, hi - stalled_samples):hi]
            if len(last) == stalled_samples and all(r.get("stalled") for r in last):
                stalled += 1
        if tracks and stalled * 2 >= len(tracks):
            return t - stalled_samples * 5000
    return None


def classes_at(ramp, ts):
    """Classes in the call at ts: the last ramp row at or before it (0 before the first)."""
    ordered = sorted((r["ts"], r["classes"]) for r in ramp if "ts" in r and "classes" in r)
    best = 0
    for t, n in ordered:
        if t <= ts:
            best = n
    return best


def evaluate(proc, recv, ramp, per_step, cpu_sat=180, sat_secs=30, freeze_max=6):
    cpu_at, peak = cpu_collapse(proc, cpu_sat, sat_secs)
    # Quality is read only from the first step on: the lesson alone is the control.
    first = min((r["ts"] for r in ramp if "ts" in r), default=None)
    q_at = quality_collapse([r for r in recv if first is not None and r.get("ts", 0) >= first], freeze_max)
    found = [(t, why) for t, why in ((cpu_at, "cpu"), (q_at, "quality")) if t is not None]
    final = max((r.get("classes", 0) for r in ramp), default=0)
    if not found:
        return {"reason": None, "at": None, "classes_at": final, "ceiling_classes": final, "cpu_peak": peak}
    at, why = min(found)
    present = classes_at(ramp, at)
    return {
        "reason": why,
        "at": at,
        "classes_at": present,
        "ceiling_classes": max(0, present - per_step),
        "cpu_peak": peak,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--proc", required=True)
    p.add_argument("--recv", required=True)
    p.add_argument("--ramp", required=True)
    p.add_argument("--per-step", type=int, default=1)
    p.add_argument("--cpu-sat", type=float, default=180)
    p.add_argument("--sat-secs", type=float, default=30)
    p.add_argument("--freeze-max", type=float, default=6)
    p.add_argument("--write")
    p.add_argument("--impl")
    p.add_argument("--arch")
    p.add_argument("--students", type=int, default=30)
    p.add_argument("--max-classes", type=int, default=40)
    a = p.parse_args()
    result = evaluate(rows_of(a.proc), rows_of(a.recv), rows_of(a.ramp), a.per_step, a.cpu_sat, a.sat_secs, a.freeze_max)
    print(json.dumps(result))
    if a.write:
        if not (a.impl and a.arch):
            print("ceiling.py: --write needs --impl and --arch", file=sys.stderr)
            return 2
        with open(a.write, "w") as f:
            json.dump(
                {
                    "impl": a.impl,
                    "arch": a.arch,
                    "ceiling_classes": result["ceiling_classes"],
                    "reason": result["reason"] or "max",
                    "cpu_peak": result["cpu_peak"],
                    "students_per_class": a.students,
                },
                f,
            )
            f.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
