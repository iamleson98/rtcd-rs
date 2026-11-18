"""Tests of quality.py: python3 -m unittest discover -s scripts/lesson-soak"""
import json
import os
import sys
import tempfile
import unittest

here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, here)
import quality  # noqa: E402


def row(ts, group, psnr, ssim, n=10, impl="rtcd-rs/chrome", height=360, worst=None):
    return {"kind": "quality", "ts": ts, "impl": impl, "arch": "aarch64", "group": group, "samples": n,
            "psnr_db": psnr, "psnr_min_db": psnr - 3 if worst is None else worst, "ssim": ssim,
            "ssim_min": ssim - 0.05, "height": height}


def phase(ts, group, name):
    return {"kind": "phase", "ts": ts, "group": group, "name": name}


class Summary(unittest.TestCase):
    def test_windows_are_weighted_by_their_samples(self):
        rows = [row(1000, "clean", 40.0, 0.99, n=30), row(6000, "clean", 30.0, 0.90, n=10)]
        (r,) = quality.summarize(rows, {})
        self.assertEqual((r["windows"], r["samples"]), (2, 40))
        self.assertAlmostEqual(r["psnr_db"], 37.5)
        self.assertAlmostEqual(r["ssim"], 0.9675)
        self.assertEqual(r["psnr_min_db"], 27.0)
        self.assertEqual(r["phase"], "-")

    def test_a_group_is_split_by_the_phase_it_was_in(self):
        phases = {"impaired": [(0, "clean"), (5000, "loss")]}
        rows = [row(1000, "impaired", 38, 0.98), row(7000, "impaired", 25, 0.80, height=180)]
        got = {r["phase"]: r for r in quality.summarize(rows, phases)}
        self.assertEqual(sorted(got), ["clean", "loss"])
        self.assertEqual(got["clean"]["large_pct"], 100)
        self.assertEqual(got["loss"]["large_pct"], 0)

    def test_other_services_and_bad_rows_stay_apart_or_out(self):
        rows = [row(1, "remote", 35, 0.95, impl="rtcd-rs/chrome-remote"), row(2, "clean", 40, 0.99),
                {"kind": "quality", "ts": 3, "impl": "x", "group": "clean", "samples": 0},
                {"kind": "quality", "ts": 4, "impl": "x", "group": "clean"}]
        self.assertEqual([r["impl"] for r in quality.summarize(rows, {})], ["rtcd-rs/chrome", "rtcd-rs/chrome-remote"])


class Files(unittest.TestCase):
    def test_a_merged_file_is_read_and_printed(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "rtcd-rs-aarch64.jsonl")
            with open(path, "w") as f:
                f.write("not json\n")
                for r in (phase(0, "impaired", "bw"), row(10, "impaired", 31.2, 0.93),
                          {"kind": "recv", "ts": 11}):
                    f.write(json.dumps(r) + "\n")
            q, p = quality.load([path])
            self.assertEqual(len(q), 1)
            text = quality.table(quality.summarize(q, p))
            self.assertIn("| rtcd-rs/chrome | aarch64 | impaired | bw | 10 | 31.2 |", text)

    def test_no_rows_say_so(self):
        self.assertIn("No picture scores", quality.table([]))


if __name__ == "__main__":
    unittest.main()
