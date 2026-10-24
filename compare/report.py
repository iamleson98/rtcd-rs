#!/usr/bin/env python3
"""Compares the Go and the Rust rtcd from the text results run-suite.sh wrote.

    compare/report.py <results dir>     # holds <os>-<go|rust>/ directories

Prints a markdown report (and appends it to $GITHUB_STEP_SUMMARY). The exit status is 1 when the
Rust server fails something the suite checks, or is worse than Go at the load it was measured at.
"""
import os
import re
import sys
from pathlib import Path


def read(d: Path, name: str) -> str:
    p = d / name
    return p.read_text(errors="replace") if p.exists() else ""


def unit(server: str, text: str):
    if server == "go":
        ok = len(re.findall(r"^ok\s", text, re.M))
        failed = len(re.findall(r"^(?:FAIL|--- FAIL)", text, re.M))
        names = sorted(set(re.findall(r"^--- FAIL: (\S+)", text, re.M)))
        return ok, failed, names
    passed = sum(int(n) for n in re.findall(r"test result: \w+\. (\d+) passed", text))
    failed = sum(int(n) for n in re.findall(r"; (\d+) failed", text))
    names = sorted(set(re.findall(r"^test (\S+) \.\.\. FAILED", text, re.M)))
    return passed, failed, names


def checks(text: str):
    return len(re.findall(r"^\s+PASS\s", text, re.M)), len(re.findall(r"^\s+FAIL\s", text, re.M))


def load(text: str):
    def num(pattern, default=None):
        m = re.search(pattern, text)
        return float(m.group(1)) if m else default

    return {
        "joined": num(r"joined (\d+) participants"),
        "failures": num(r"\((\d+) failures\)", None),
        "join_p50": num(r"join time\s+p50 ([\d.]+)s"),
        "join_p95": num(r"p95 ([\d.]+)s"),
        "voice": num(r"voice delivery ([\d.]+)%"),
        "camera": num(r"camera\s+([\d.]+)%"),
        "cpu": num(r"server cpu\s+([\d.]+)%"),
        "rss": num(r"server memory\s+(\d+) MiB"),
    }


def fmt(v, unit="", digits=1):
    return "n/a" if v is None else f"{v:.{digits}f}{unit}"


def main() -> int:
    root = Path(sys.argv[1])
    out, problems = ["# rtcd-go vs rtcd-rust\n"], []
    for osdir in sorted({p.name.rsplit("-", 1)[0] for p in root.glob("*-*") if p.is_dir()}):
        d = {s: root / f"{osdir}-{s}" for s in ("go", "rust")}
        if not all(p.is_dir() for p in d.values()):
            out.append(f"## {osdir}\nmissing results for one of the servers\n")
            problems.append(f"{osdir}: results missing")
            continue
        out.append(f"## {osdir}\n")
        out.append("| | Go | Rust |\n|---|---|---|")
        u = {s: unit(s, read(d[s], "unit.txt")) for s in d}
        out.append(f"| unit tests (ok / failed) | {u['go'][0]} / {u['go'][1]} | {u['rust'][0]} / {u['rust'][1]} |")
        for what, name in (("interop checks", "interop.txt"), ("browser checks", "browser.txt")):
            c = {s: checks(read(d[s], name)) for s in d}
            out.append(f"| {what} (pass / fail) | {c['go'][0]} / {c['go'][1]} | {c['rust'][0]} / {c['rust'][1]} |")
            if c["rust"][1] or c["rust"][0] == 0 or "all checks passed" not in read(d["rust"], name):
                problems.append(f"{osdir}: Rust {what} failed or did not finish ({c['rust'][1]} failing, {c['rust'][0]} passing)")
        if u["rust"][1] or u["rust"][0] == 0:
            problems.append(f"{osdir}: Rust unit tests failed: {', '.join(u['rust'][2]) or 'none ran'}")
        for label, name in (("90 participants (3 classes)", "load3.txt"), ("300 participants (10 classes)", "load10.txt")):
            l = {s: load(read(d[s], name)) for s in d}
            out.append(f"| **{label}** | | |")
            for key, title, unit_, digits in (
                ("failures", "join failures", "", 0),
                ("join_p50", "join time p50", " s", 2),
                ("join_p95", "join time p95", " s", 2),
                ("voice", "voice delivered", " %", 2),
                ("camera", "camera delivered", " %", 2),
                ("cpu", "server CPU (% of a core)", " %", 1),
                ("rss", "server memory", " MiB", 0),
            ):
                out.append(f"| {title} | {fmt(l['go'][key], unit_, digits)} | {fmt(l['rust'][key], unit_, digits)} |")
            r, g = l["rust"], l["go"]
            if r["joined"] is None or r["failures"]:
                problems.append(f"{osdir}: Rust had join failures in {name}")
            for key in ("voice", "camera"):
                if r[key] is None or r[key] < 99.0:
                    problems.append(f"{osdir}: Rust {key} delivery {fmt(r[key], '%')} in {name}")
            if name == "load10.txt":
                if None not in (r["cpu"], g["cpu"]) and r["cpu"] > g["cpu"]:
                    problems.append(f"{osdir}: Rust uses more CPU than Go ({r['cpu']}% vs {g['cpu']}%)")
                if None not in (r["rss"], g["rss"]) and r["rss"] > g["rss"]:
                    problems.append(f"{osdir}: Rust uses more memory than Go ({r['rss']} vs {g['rss']} MiB)")
        sweep = [(n, load(read(d["rust"], f"load10-s{n}.txt"))) for n in (1, 2, 4)]
        if any(l["cpu"] is not None for _, l in sweep):
            out.append("\n**Rust, 300 participants, by shard count**\n")
            out.append("| shards | CPU (% of a core) | memory | voice delivered | camera delivered |\n|---|---|---|---|---|")
            for n, l in sweep:
                out.append(f"| {n} | {fmt(l['cpu'], ' %')} | {fmt(l['rss'], ' MiB', 0)} | {fmt(l['voice'], ' %', 2)} | {fmt(l['camera'], ' %', 2)} |")
        if u["go"][2]:
            out.append(f"\nGo tests failing here: {', '.join(u['go'][2])}\n")
        out.append("")
    out.append("## Verdict\n")
    out.append("Rust is at least as good as Go on everything checked." if not problems else "\n".join(f"- {p}" for p in problems))
    text = "\n".join(out) + "\n"
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(text)
    (root / "report.md").write_text(text)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
