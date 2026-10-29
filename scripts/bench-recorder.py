#!/usr/bin/env python3
"""Compares the Go session recorder with this one on the same synthetic classes.

    scripts/bench-recorder.py --go ~/Documents/class-management/recorder [--runs 3] [--quick]

Both recorders record the same class (lms-sim: a real rtcd, participants sending real VP8 and Opus)
and are measured by /usr/bin/time: the CPU seconds and the peak memory of the whole process, from
start to the end of its upload. The Go recorder is built from a copy of its source in which only the
hard-coded /data directory is made configurable, so it can run outside a container.
"""
import argparse
import os
import json
import shutil
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# people, cameras, screens, seconds
SCENARIOS = [
    ("small class", 3, 2, 1, 15),
    ("class of 10", 10, 6, 1, 30),
    ("class of 20", 20, 12, 1, 40),
    ("class of 30", 30, 20, 2, 60),
]


def build_go(src: Path, work: Path) -> Path:
    copy = work / "go-recorder"
    shutil.copytree(src, copy, ignore=shutil.ignore_patterns("recorder", "*_test.go", ".git"))
    main = copy / "main.go"
    text = main.read_text()
    text = text.replace('\t// dataDir holds the recording until it is uploaded.\n\tdataDir = "/data"\n', "")
    main.write_text(text)
    (copy / "bench_patch.go").write_text(
        'package main\n\nimport "os"\n\nvar dataDir = func() string {\n'
        '\tif d := os.Getenv("RECORDER_DATA_DIR"); d != "" {\n\t\treturn d\n\t}\n\treturn "/data"\n}()\n'
    )
    out = work / "recorder-go"
    subprocess.run(["go", "build", "-trimpath", "-ldflags=-s -w", "-o", str(out), "."], cwd=copy, check=True)
    return out


def build_rust() -> tuple[Path, Path]:
    subprocess.run(["cargo", "build", "--release", "-p", "recorder", "-p", "lms-sim"], cwd=ROOT, check=True)
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    return target / "release/recorder", target / "release/lms-sim"


def run(sim: Path, recorder: Path, label: str, scenario, out: Path) -> dict:
    _, people, cameras, screens, seconds = scenario
    proc = subprocess.run(
        [str(sim), "run", "--recorder", str(recorder), "--label", label, "--duration", str(seconds),
         "--people", str(people), "--cameras", str(cameras), "--screens", str(screens), "--out", str(out)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        sys.exit(f"{label}: lms-sim failed\n{proc.stderr}")
    return json.loads(proc.stdout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--go", type=Path, help="the Go recorder's source directory")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--quick", action="store_true", help="the first two scenarios only, once")
    args = ap.parse_args()
    scenarios = SCENARIOS[:2] if args.quick else SCENARIOS
    runs = 1 if args.quick else args.runs

    work = Path(tempfile.mkdtemp(prefix="bench-recorder-"))
    rust, sim = build_rust()
    go = build_go(args.go, work) if args.go else None
    recorders = ([("go", go)] if go else []) + [("rust", rust)]

    rows = []
    for sc in scenarios:
        results = {}
        for label, binary in recorders:
            rs = [run(sim, binary, label, sc, work / f"{label}-{sc[1]}-{i}") for i in range(runs)]
            for r in rs:
                if r["exit"] != 0 or r["failed"] or not r["recorder_joined"]:
                    sys.exit(f"{label} {sc[0]}: {json.dumps({k: r[k] for k in ('exit', 'failed', 'recorder_joined')})}")
            media = rs[0]["files"]
            results[label] = {
                "cpu": statistics.median(r["cpu_s"] for r in rs),
                "rss": statistics.median(r["max_rss_mib"] for r in rs),
                "files": len(media),
                "bytes": statistics.median(sum(f["size"] for f in r["files"]) for r in rs) / 1048576,
            }
        rows.append((sc, results))

    print(f"\n| class (people / cameras / screens, seconds) | recorder | CPU seconds | CPU % of a core | peak memory | files | MiB written |")
    print("|---|---|---|---|---|---|---|")
    for sc, res in rows:
        for label, _ in recorders:
            r = res[label]
            print(f"| {sc[0]} ({sc[1]} / {sc[2]} / {sc[3]}, {sc[4]} s) | {label} | {r['cpu']:.2f} | {100 * r['cpu'] / sc[4]:.1f}% | {r['rss']:.1f} MiB | {r['files']} | {r['bytes']:.1f} |")
    if go:
        print()
        for sc, res in rows:
            g, r = res["go"], res["rust"]
            print(f"{sc[0]}: Rust uses {g['cpu'] / r['cpu']:.1f}x less CPU and {g['rss'] / r['rss']:.1f}x less memory")
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
