#!/usr/bin/env python3
"""Receiver-side quality of lab runs, from the node's own counters: quality.py out/A.jsonl ..."""
import json
import sys

print(f"{'run':24} {'loss%':>7} {'nack':>8} {'rtx_lim':>8} {'cpu_pk':>7} {'pps_pk':>8}")
for path in sys.argv[1:]:
    rs = [json.loads(l) for l in open(path)]
    n = max(r.get("rtcd_rtc_receiver_loss_ratio_count", 0) for r in rs)
    t = max(r.get("rtcd_rtc_receiver_loss_ratio_sum", 0) for r in rs)
    nack = max(r.get('rtcd_rtc_receiver_feedback_total{type="nack"}', 0) for r in rs)
    lim = max(r.get("rtcd_rtc_retransmits_limited_total", 0) for r in rs)
    cpu = max(r.get("cpu_load", 0) for r in rs)
    pps = max(r.get("egress_pps", 0) for r in rs)
    name = path.rsplit("/", 1)[-1].replace(".jsonl", "")
    print(f"{name:24} {100 * t / max(n, 1):7.2f} {int(nack):8} {int(lim):8} {cpu:7.2f} {int(pps):8}")
