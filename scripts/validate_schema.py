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
        # `jinliang export --all` 用 `---` 分隔多个任务；只 safe_load 会报
        # "expected a single document"。这里逐个文档校验，空文档跳过。
        docs = [d for d in yaml.safe_load_all(pathlib.Path(p).read_text(encoding="utf-8")) if d is not None]
        if not docs:
            print(f"{p}: INVALID -> 空文档", file=sys.stderr)
            rc = 1
            continue
        bad = False
        for i, doc in enumerate(docs):
            where = f"{p}#doc{i + 1}" if len(docs) > 1 else p
            try:
                jsonschema.validate(doc, SCHEMA)
            except jsonschema.ValidationError as e:
                print(f"{where}: INVALID -> {e.message} (at {list(e.path)})", file=sys.stderr)
                rc = 1
                bad = True
                break
        if not bad:
            print(f"{p}: ok" + (f"（{len(docs)} 份）" if len(docs) > 1 else ""))
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
