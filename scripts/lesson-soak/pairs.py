#!/usr/bin/env python3
"""The rules file for the services a run has: rules.json's pairs with the right subject.

    pairs.py RULES.json OUT.json IMPL...        (IMPL: rs, rs-budget, go)

The gated subject is rtcd-rs-budget when that soak ran, else rtcd-rs, held against the Go rtcd (the
pairs of RULES.json with their rtcd-rs replaced). When both rs and rs-budget ran, rs-budget is also
held against rs, for the report only (`"gate": false`: a breach shows in the tables and does not fail).
"""
import json
import sys

SUBJECT = "rtcd-rs"
BUDGET = "rtcd-rs-budget"


def build(rules, impls):
    gated = BUDGET if "rs-budget" in impls else SUBJECT
    pairs = []
    for p in rules["compare"]:
        q = dict(p)
        q["subject"] = gated + p["subject"][len(SUBJECT):]
        pairs.append(q)
    if "rs-budget" in impls and "rs" in impls:
        for p in rules["compare"]:
            pairs.append({**p, "subject": BUDGET + p["subject"][len(SUBJECT):],
                          "baseline": SUBJECT + p["subject"][len(SUBJECT):], "gate": False})
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
