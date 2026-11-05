#!/usr/bin/env python3
"""Validate job.yaml against schema/job.schema.json using only the stdlib.

为什么有第二个校验器：CI 里用 jsonschema（权威），但沙盒/离线环境常常装不上
pyyaml 与 jsonschema，而"导出的 yaml 能不能过 schema"是一条要能当场验证的验收项。
这个文件实现 job.schema.json 实际用到的那一小撮关键字，够用就好；两者结论不一致
时以 scripts/validate_schema.py（jsonschema）为准。

用法: python3 scripts/validate_schema_lite.py examples/*.yaml
"""
from __future__ import annotations

import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCHEMA = json.loads((ROOT / "schema" / "job.schema.json").read_text(encoding="utf-8"))


# ── 最小 YAML 子集解析器 ──────────────────────────────────────
# job.yaml 只用到：映射、序列、标量、行内 {}/[]、注释、引号字符串、多文档分隔。
# 这个解析器刻意保持"够用且严格"：不懂的写法直接报错，而不是猜。

class YamlError(ValueError):
    pass


def _strip_comment(line: str) -> str:
    out, quote, i = [], None, 0
    while i < len(line):
        ch = line[i]
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in "\"'":
            quote = ch
            out.append(ch)
        elif ch == "#" and (not out or out[-1] in " \t"):
            break
        else:
            out.append(ch)
        i += 1
    return "".join(out).rstrip()


def _scalar(tok: str):
    tok = tok.strip()
    if tok == "" or tok == "~" or tok == "null":
        return None
    if tok[0] in "\"'" and tok[-1] == tok[0] and len(tok) >= 2:
        return tok[1:-1]
    if tok.startswith("[") and tok.endswith("]"):
        inner = tok[1:-1].strip()
        return [] if not inner else [_scalar(p) for p in _split_inline(inner)]
    if tok.startswith("{") and tok.endswith("}"):
        inner = tok[1:-1].strip()
        if not inner:
            return {}
        out = {}
        for part in _split_inline(inner):
            k, _, v = part.partition(":")
            out[str(_scalar(k))] = _scalar(v)
        return out
    if tok in ("true", "True"):
        return True
    if tok in ("false", "False"):
        return False
    try:
        return int(tok)
    except ValueError:
        pass
    try:
        return float(tok)
    except ValueError:
        pass
    return tok


def _split_inline(s: str) -> list[str]:
    parts, buf, depth, quote = [], [], 0, None
    for ch in s:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in "\"'":
            quote = ch
            buf.append(ch)
        elif ch in "[{":
            depth += 1
            buf.append(ch)
        elif ch in "]}":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if "".join(buf).strip():
        parts.append("".join(buf).strip())
    return parts


def parse_yaml(text: str) -> dict:
    lines = []
    for raw in text.splitlines():
        if raw.strip().startswith("#") or raw.strip() in ("---", "..."):
            continue
        line = _strip_comment(raw)
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        lines.append((indent, line.strip()))
    if not lines:
        raise YamlError("空文档")
    value, idx = _parse_block(lines, 0, lines[0][0])
    if idx != len(lines):
        raise YamlError("第 %d 行无法归入任何层级（缩进不一致？）" % (idx + 1))
    return value


def _parse_block(lines, idx, indent):
    if idx >= len(lines):
        return None, idx
    if lines[idx][1].startswith("- "):
        out = []
        while idx < len(lines) and lines[idx][0] == indent and lines[idx][1].startswith("- "):
            item = lines[idx][1][2:].strip()
            idx += 1
            if not item:
                nested, idx = _parse_block(lines, idx, lines[idx][0] if idx < len(lines) else indent + 2)
                out.append(nested)
                continue
            if ":" in item and not item.startswith(("{", "[", '"', "'")):
                # "- key: value" 是「列表项内的映射」，把同层的后续行一起交给映射解析
                item_indent = indent + 2
                virtual = [(item_indent, item)]
                while idx < len(lines) and lines[idx][0] > indent:
                    virtual.append(lines[idx])
                    idx += 1
                child, _ = _parse_block(virtual, 0, item_indent)
                out.append(child)
            else:
                out.append(_scalar(item))
        return out, idx
    out = {}
    while idx < len(lines) and lines[idx][0] == indent:
        content = lines[idx][1]
        if ":" not in content:
            raise YamlError("不是 key: value 形式：%r" % content)
        key, _, rest = content.partition(":")
        key = str(_scalar(key))
        rest = rest.strip()
        idx += 1
        if rest:
            out[key] = _scalar(rest)
        else:
            if idx < len(lines) and lines[idx][0] > indent:
                child, idx = _parse_block(lines, idx, lines[idx][0])
                out[key] = child
            elif idx < len(lines) and lines[idx][0] == indent and lines[idx][1].startswith("- "):
                child, idx = _parse_block(lines, idx, indent)
                out[key] = child
            else:
                out[key] = None
    return out, idx


# ── 最小 JSON Schema 校验器 ───────────────────────────────────

TYPES = {
    "object": dict,
    "array": list,
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
}


def _resolve(schema, root):
    while "$ref" in schema:
        ref = schema["$ref"]
        if not ref.startswith("#/"):
            raise YamlError("只支持本地 $ref：%s" % ref)
        node = root
        for part in ref[2:].split("/"):
            node = node[part]
        schema = node
    return schema


def validate(value, schema, root, path="$"):
    schema = _resolve(schema, root)
    errs = []

    if "const" in schema and value != schema["const"]:
        errs.append("%s: 必须是 %r（实际 %r）" % (path, schema["const"], value))
    if "enum" in schema and value not in schema["enum"]:
        errs.append("%s: 必须是 %s 之一（实际 %r）" % (path, schema["enum"], value))

    t = schema.get("type")
    if t:
        allowed = TYPES[t]
        ok = isinstance(value, allowed)
        if t in ("integer", "number") and isinstance(value, bool):
            ok = False
        if t == "integer" and isinstance(value, float) and value.is_integer():
            ok = True
        if not ok:
            errs.append("%s: 类型应为 %s（实际 %s）" % (path, t, type(value).__name__))
            return errs

    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            errs.append("%s: 至少 %d 个字符" % (path, schema["minLength"]))
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            errs.append("%s: 最多 %d 个字符" % (path, schema["maxLength"]))
        if "pattern" in schema and not re.search(schema["pattern"], value):
            errs.append("%s: 不匹配 %s（实际 %r）" % (path, schema["pattern"], value))
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            errs.append("%s: 不能小于 %s" % (path, schema["minimum"]))
        if "maximum" in schema and value > schema["maximum"]:
            errs.append("%s: 不能大于 %s" % (path, schema["maximum"]))

    if isinstance(value, dict):
        for req in schema.get("required", []):
            if req not in value:
                errs.append("%s: 缺少必填字段 %r" % (path, req))
        props = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            for k in value:
                if k not in props:
                    errs.append("%s: 不允许的字段 %r" % (path, k))
        pattern_props = schema.get("patternProperties", {})
        for k, v in value.items():
            if k in props:
                errs += validate(v, props[k], root, "%s.%s" % (path, k))
            else:
                for pat, sub in pattern_props.items():
                    if re.search(pat, k):
                        errs += validate(v, sub, root, "%s.%s" % (path, k))
                        break
                else:
                    extra = schema.get("additionalProperties")
                    if isinstance(extra, dict):
                        errs += validate(v, extra, root, "%s.%s" % (path, k))
        if "minProperties" in schema and len(value) < schema["minProperties"]:
            errs.append("%s: 至少要有 %d 个字段" % (path, schema["minProperties"]))

    if isinstance(value, list):
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for i, item in enumerate(value):
                errs += validate(item, item_schema, root, "%s[%d]" % (path, i))

    for sub in schema.get("allOf", []):
        errs += validate(value, sub, root, path)

    if "if" in schema and "then" in schema:
        if not validate(value, schema["if"], root, path):
            errs += validate(value, schema["then"], root, path)

    return errs


def main(paths: list[str]) -> int:
    if not paths:
        print("no files given", file=sys.stderr)
        return 2
    rc = 0
    for p in paths:
        try:
            doc = parse_yaml(pathlib.Path(p).read_text(encoding="utf-8"))
        except (YamlError, OSError) as e:
            print("%s: YAML 解析失败 -> %s" % (p, e), file=sys.stderr)
            rc = 1
            continue
        errs = validate(doc, SCHEMA, SCHEMA)
        if errs:
            print("%s: INVALID" % p, file=sys.stderr)
            for e in errs:
                print("  - %s" % e, file=sys.stderr)
            rc = 1
        else:
            print("%s: ok" % p)
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
