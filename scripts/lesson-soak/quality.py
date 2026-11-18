#!/usr/bin/env python3
"""The picture score of a lesson soak as a table: how close the teacher's camera the Chromium clients
received was to the clip it was sent from (PSNR and SSIM of luma; see README.md, "Picture quality").

    quality.py FILE.jsonl... > quality.md

Reads the `quality` rows (one per Chromium and 5 seconds: `samples`, `psnr_db`, `psnr_min_db`, `ssim`,
`ssim_min`, `height`) and the netem `phase` rows, and prints one row per service, architecture, group
and network phase. It only shows values: nothing here gates a run (report.py, which does, has no
picture rules yet). Rows of other kinds are ignored, so it takes the merged `rtcd-*.jsonl` files as
they are.
"""
import json
import sys


def load(paths):
    quality, phases = [], {}
    for path in paths:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(row, dict) or not isinstance(row.get("ts"), (int, float)):
                    continue
                if row.get("kind") == "quality":
                    quality.append(row)
                elif row.get("kind") == "phase" and row.get("group"):
                    phases.setdefault(row["group"], []).append((row["ts"], str(row.get("name"))))
    for series in phases.values():
        series.sort()
    return quality, phases


def phase_at(phases, group, ts):
    """The netem phase of a group at `ts` (the last one that began), `-` for a group nothing shapes."""
    name = "-"
    for at, n in phases.get(group, []):
        if at > ts:
            break
        name = n
    return name


def summarize(quality, phases):
    """One entry per (impl, arch, group, phase): the windows' samples weighted into means, and the
    worst window."""
    cells = {}
    for r in quality:
        try:
            n = int(r["samples"])
            row = (float(r["psnr_db"]), float(r["psnr_min_db"]), float(r["ssim"]), float(r["ssim_min"]))
        except (KeyError, TypeError, ValueError):
            continue
        if n <= 0:
            continue
        group = str(r.get("group", "?"))
        key = (str(r.get("impl")), str(r.get("arch", "?")), group, phase_at(phases, group, r["ts"]))
        c = cells.setdefault(key, {"windows": 0, "samples": 0, "psnr": 0.0, "ssim": 0.0,
                                   "psnr_min": 99.0, "ssim_min": 1.0, "large": 0})
        c["windows"] += 1
        c["samples"] += n
        c["psnr"] += row[0] * n
        c["ssim"] += row[2] * n
        c["psnr_min"] = min(c["psnr_min"], row[1])
        c["ssim_min"] = min(c["ssim_min"], row[3])
        c["large"] += 1 if r.get("height", 0) >= 360 else 0
    out = []
    for key in sorted(cells):
        c = cells[key]
        out.append({
            "impl": key[0], "arch": key[1], "group": key[2], "phase": key[3],
            "windows": c["windows"], "samples": c["samples"],
            "psnr_db": c["psnr"] / c["samples"], "psnr_min_db": c["psnr_min"],
            "ssim": c["ssim"] / c["samples"], "ssim_min": c["ssim_min"],
            "large_pct": 100.0 * c["large"] / c["windows"],
        })
    return out


def table(rows):
    if not rows:
        return "No picture scores (no `quality` rows: the Chromium page could not decode its reference).\n"
    head = ("| service | arch | group | phase | frames | PSNR dB | worst PSNR | SSIM | worst SSIM | large layer |\n"
            "|---|---|---|---|---:|---:|---:|---:|---:|---:|\n")
    body = "".join(
        f"| {r['impl']} | {r['arch']} | {r['group']} | {r['phase']} | {r['samples']} | {r['psnr_db']:.1f} | "
        f"{r['psnr_min_db']:.1f} | {r['ssim']:.3f} | {r['ssim_min']:.3f} | {r['large_pct']:.0f} % |\n"
        for r in rows)
    note = ("\nPSNR and SSIM of luma at 320x180 against the clip of the layer it was sent from (60 dB is identical); `worst` is the "
            "lowest 5-second window; `large layer` is the share of windows shown at 640x360.\n")
    return head + body + note


def main(argv):
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    quality, phases = load(argv)
    sys.stdout.write("## Picture quality\n\n" + table(summarize(quality, phases)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
