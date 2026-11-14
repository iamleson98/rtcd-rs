#!/usr/bin/env python3
"""The rules file for the services a run has: rules.json's pairs with the right subject.

    pairs.py RULES.json OUT.json IMPL...        (IMPL: rs, rs-budget, go)

rtcd-rs as it ships (rs, the camera budget off) is the gated subject whenever it ran: it is held
against the Go rtcd (the pairs of RULES.json). The camera budget's soak (rs-budget) is held against
the Go rtcd too, and against rs when that ran, for the report only (`"gate": false`: a breach shows
in the tables and does not fail) while rs is gated; without rs, rs-budget is the gated subject.
"""
import json
import sys

SUBJECT = "rtcd-rs"
BUDGET = "rtcd-rs-budget"


def build(rules, impls):
    plain = "rs" in impls
    pairs = [dict(p) for p in rules["compare"]] if plain else []
    if "rs-budget" in impls:
        budget = [{**p, "subject": BUDGET + p["subject"][len(SUBJECT):]} for p in rules["compare"]]
        if plain:
            pairs += [{**p, "gate": False} for p in budget]
            pairs += [{**p, "baseline": SUBJECT + p["subject"][len(BUDGET):], "gate": False}
                      for p in budget]
        else:
            pairs += budget
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
