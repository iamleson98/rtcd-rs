#!/usr/bin/env python3
"""The load both servers carry in the abuse profile: 90 % of the lower of their ceilings.

    hold.py ARCH CEILING_RS.json CEILING_GO.json [--out FILE]

Prints `hold_classes=N` (also written to --out), N = floor(0.9 x min(ceilings)), the same for both
implementations on that architecture, so that neither is held to a load the other could not carry.
Exits 1, saying why, when a ceiling file is missing or unreadable or is for another architecture.
"""
import argparse
import json
import math
import sys


def ceiling(path, arch):
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        raise SystemExit("hold.py: cannot read the ceiling file %s: %s" % (path, e))
    if not isinstance(data.get("ceiling_classes"), int) or data["ceiling_classes"] < 0:
        raise SystemExit("hold.py: %s has no ceiling_classes" % path)
    if data.get("arch") not in (arch, None):
        raise SystemExit("hold.py: %s is for %s, not %s" % (path, data.get("arch"), arch))
    return data["ceiling_classes"]


def hold_classes(ceilings):
    # Integer arithmetic: 0.9 x 10 must be 9, not 8.999999999999998.
    return math.floor(min(ceilings) * 9 / 10)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("arch")
    p.add_argument("files", nargs=2)
    p.add_argument("--out")
    a = p.parse_args()
    n = hold_classes([ceiling(f, a.arch) for f in a.files])
    line = "hold_classes=%d" % n
    print(line)
    if a.out:
        with open(a.out, "w") as f:
            f.write(line + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
