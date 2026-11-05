#!/usr/bin/env python3
"""Validate job.yaml files against schema/job.schema.json.

Usage: python scripts/validate_schema.py examples/*.yaml
Exits non-zero on the first invalid document.
"""
from __future__ import annotations

import json
import pathlib
import sys

import jsonschema
import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCHEMA = json.loads((ROOT / "schema" / "job.schema.json").read_text(encoding="utf-8"))


def main(paths: list[str]) -> int:
    if not paths:
        print("no files given", file=sys.stderr)
        return 2
    rc = 0
    for p in paths:
        doc = yaml.safe_load(pathlib.Path(p).read_text(encoding="utf-8"))
        try:
            jsonschema.validate(doc, SCHEMA)
        except jsonschema.ValidationError as e:
            print(f"{p}: INVALID -> {e.message} (at {list(e.path)})", file=sys.stderr)
            rc = 1
        else:
            print(f"{p}: ok")
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
