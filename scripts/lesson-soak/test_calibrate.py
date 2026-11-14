"""Tests of ceiling.py, hold.py and pairs.py: python3 -m unittest discover -s scripts/lesson-soak"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import ceiling  # noqa: E402
import hold  # noqa: E402
import pairs  # noqa: E402


def proc(cpus, start=0):
    return [{"kind": "proc", "ts": start + i * 5000, "cpu_pct": c} for i, c in enumerate(cpus)]


def recv(track, media, freezes, stalled=None, start=0):
    rows = []
    for i, f in enumerate(freezes):
        rows.append({"kind": "recv", "id": track, "media": media, "ts": start + i * 5000, "freezes": f,
                     "stalled": bool(stalled and stalled[i])})
    return rows


class Cpu(unittest.TestCase):
    def test_saturation_needs_the_whole_stretch(self):
        at, peak = ceiling.cpu_collapse(proc([100, 185, 190, 100, 185, 190, 185, 188, 190, 186, 190, 188, 190]), 180, 30)
        self.assertEqual(at, 4 * 5000)
        self.assertEqual(peak, 190)

    def test_a_short_spike_is_not_saturation(self):
        at, _ = ceiling.cpu_collapse(proc([100, 190, 190, 190, 100, 190, 190]), 180, 30)
        self.assertIsNone(at)


class Quality(unittest.TestCase):
    def test_steady_receivers_do_not_collapse(self):
        rows = recv("a", "video", [0] * 30) + recv("a", "audio", [0] * 30)
        self.assertIsNone(ceiling.quality_collapse(rows, 6))

    def test_freezing_receivers_collapse(self):
        rows = recv("a", "video", [i for i in range(30)])  # 12 freezes a minute
        self.assertIsNotNone(ceiling.quality_collapse(rows, 6))

    def test_stalled_tracks_collapse(self):
        rows = recv("a", "video", [0] * 10, [False] * 5 + [True] * 5)
        self.assertIsNotNone(ceiling.quality_collapse(rows, 6))


class Evaluate(unittest.TestCase):
    ramp = [{"ts": 10000 * n, "classes": n} for n in range(1, 11)]

    def test_cpu_collapse_gives_the_classes_before_the_step(self):
        # Saturation from 5 s after the 4th step began (ts 40000 s -> 45000).
        samples = proc([100] * 9 + [195] * 8)  # saturated from ts 45000
        result = ceiling.evaluate(samples, [], self.ramp, 1)
        self.assertEqual(result["reason"], "cpu")
        self.assertEqual(result["classes_at"], 4)
        self.assertEqual(result["ceiling_classes"], 3)

    def test_per_step_is_taken_off(self):
        result = ceiling.evaluate(proc([100] * 9 + [195] * 8), [], self.ramp, 2)
        self.assertEqual(result["ceiling_classes"], 2)

    def test_no_collapse_is_max(self):
        result = ceiling.evaluate(proc([100] * 20), recv("a", "video", [0] * 20), self.ramp, 1)
        self.assertIsNone(result["reason"])
        self.assertEqual(result["ceiling_classes"], 10)

    def test_freezes_before_the_ramp_are_not_counted(self):
        rows = recv("a", "video", [i for i in range(30)])
        ramp = [{"ts": 10**9, "classes": 1}]
        self.assertIsNone(ceiling.evaluate([], rows, ramp, 1)["reason"])

    def test_collapse_in_the_first_step_is_zero(self):
        result = ceiling.evaluate(proc([195] * 10), [], [{"ts": 0, "classes": 1}], 1)
        self.assertEqual(result["ceiling_classes"], 0)


class Files(unittest.TestCase):
    def write(self, d, name, data):
        path = os.path.join(d, name)
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    def test_hold_is_ninety_percent_of_the_lower_ceiling(self):
        with tempfile.TemporaryDirectory() as d:
            rs = self.write(d, "rs.json", {"arch": "x64", "ceiling_classes": 12})
            go = self.write(d, "go.json", {"arch": "x64", "ceiling_classes": 10})
            out = os.path.join(d, "hold")
            run = subprocess.run([sys.executable, os.path.join(here, "hold.py"), "x64", rs, go, "--out", out],
                                 capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual(run.stdout.strip(), "hold_classes=9")
            with open(out) as f:
                self.assertEqual(f.read().strip(), "hold_classes=9")

    def test_floor(self):
        self.assertEqual(hold.hold_classes([7, 30]), 6)
        self.assertEqual(hold.hold_classes([1]), 0)

    def test_a_missing_ceiling_fails_loudly(self):
        with tempfile.TemporaryDirectory() as d:
            rs = self.write(d, "rs.json", {"arch": "x64", "ceiling_classes": 12})
            run = subprocess.run([sys.executable, os.path.join(here, "hold.py"), "x64", rs, os.path.join(d, "none.json")],
                                 capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)
            self.assertIn("none.json", run.stderr)

    def test_another_arch_fails(self):
        with tempfile.TemporaryDirectory() as d:
            rs = self.write(d, "rs.json", {"arch": "x64", "ceiling_classes": 12})
            go = self.write(d, "go.json", {"arch": "arm64", "ceiling_classes": 12})
            run = subprocess.run([sys.executable, os.path.join(here, "hold.py"), "x64", rs, go], capture_output=True, text=True)
            self.assertNotEqual(run.returncode, 0)

    def test_write_ceiling_file(self):
        with tempfile.TemporaryDirectory() as d:
            for name, rows in (("p", proc([100] * 20)), ("r", []), ("m", [{"ts": 0, "classes": 5}])):
                with open(os.path.join(d, name), "w") as f:
                    f.write("".join(json.dumps(r) + "\n" for r in rows))
            out = os.path.join(d, "ceiling.json")
            run = subprocess.run(
                [sys.executable, os.path.join(here, "ceiling.py"), "--proc", os.path.join(d, "p"), "--recv",
                 os.path.join(d, "r"), "--ramp", os.path.join(d, "m"), "--write", out, "--impl", "rtcd-rs",
                 "--arch", "x64", "--students", "30"], capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stderr)
            with open(out) as f:
                data = json.load(f)
            self.assertEqual((data["reason"], data["ceiling_classes"], data["students_per_class"]), ("max", 5, 30))


if __name__ == "__main__":
    unittest.main()


class Pairs(unittest.TestCase):
    rules = {"compare": [{"subject": "rtcd-rs", "baseline": "rtcd-go"},
                         {"subject": "rtcd-rs/chrome", "baseline": "rtcd-go/chrome", "clean_zero": ["stalls"]}],
             "ratio": 1.25}

    def test_rs_is_gated_against_go_and_the_budget_soak_is_report_only(self):
        got = pairs.build(self.rules, ["rs", "rs-budget", "go"])["compare"]
        self.assertEqual([(p["subject"], p["baseline"], p.get("gate", True)) for p in got], [
            ("rtcd-rs", "rtcd-go", True), ("rtcd-rs/chrome", "rtcd-go/chrome", True),
            ("rtcd-rs-budget", "rtcd-go", False), ("rtcd-rs-budget/chrome", "rtcd-go/chrome", False),
            ("rtcd-rs-budget", "rtcd-rs", False), ("rtcd-rs-budget/chrome", "rtcd-rs/chrome", False)])
        self.assertEqual(got[5]["clean_zero"], ["stalls"])

    def test_without_the_budget_soak_rs_is_held_against_go(self):
        got = pairs.build(self.rules, ["rs", "go"])
        self.assertEqual(got, self.rules)

    def test_without_the_plain_soak_there_is_nothing_report_only(self):
        got = pairs.build(self.rules, ["rs-budget", "go"])["compare"]
        self.assertEqual([(p["subject"], p.get("gate", True)) for p in got],
                         [("rtcd-rs-budget", True), ("rtcd-rs-budget/chrome", True)])
