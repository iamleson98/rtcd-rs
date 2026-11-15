#!/usr/bin/env python3
"""The rules file for the services a run has: rules.json's pairs with the right subject.

    pairs.py RULES.json OUT.json IMPL...        (IMPL: rs, rs-budget, go)

rtcd-rs as it ships is the gated subject: held against the Go rtcd (the pairs of RULES.json with
their rtcd-rs replaced). It ships with the camera budget on (rs-budget); without that leg, rs is
gated instead. The other leg, when it ran, is in the report only (`"gate": false`: a breach shows in
the tables and does not fail), against the Go rtcd and against the shipped leg.
"""
import json
import sys

SUBJECT = "rtcd-rs"
LABELS = {"rs": "rtcd-rs", "rs-budget": "rtcd-rs-budget"}
# The leg that runs rtcd-rs as it ships (`camera_budget` is on by default in rtcd-sfu's config).
SHIPPED = "rs-budget"


def build(rules, impls):
    legs = [leg for leg in LABELS if leg in impls]
    if not legs:
        return rules
    gated = SHIPPED if SHIPPED in legs else legs[0]

    def as_subject(p, leg):
        return {**p, "subject": LABELS[leg] + p["subject"][len(SUBJECT):]}

    pairs = [as_subject(p, gated) for p in rules["compare"]]
    for leg in legs:
        if leg == gated:
            continue
        for p in rules["compare"]:
            q = as_subject(p, leg)
            pairs.append({**q, "gate": False})
            pairs.append({**q, "baseline": LABELS[gated] + p["subject"][len(SUBJECT):], "gate": False})
    return {**rules, "compare": pairs}


def main(argv):
    if len(argv) < 4:
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as f:
        rules = json.load(f)
    with open(argv[2], "w", encoding="utf-8") as f:
        json.dump(build(rules, argv[3:]), f, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
