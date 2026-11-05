#!/usr/bin/env bash
# 单二进制体积守门：docs/BUILD.md §1 说产物要 <25 MB。
#
# 用发布构建参数（-trimpath -ldflags "-s -w"）量，因为那才是发布出去的东西；
# 开发时不加 -s -w 会大 ~9 MB，那不是预算问题。
set -euo pipefail

LIMIT_MB="${LIMIT_MB:-25}"
WARN_MB="${WARN_MB:-22}"
OUT="${1:-dist/jinliang}"

mkdir -p "$(dirname "${OUT}")"
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=sizecheck" -o "${OUT}" ./cmd/jinliang

bytes=$(wc -c <"${OUT}")
mb=$((bytes / 1024 / 1024))
printf 'binary %s: %s bytes (%s MB), limit %s MB\n' "${OUT}" "${bytes}" "${mb}" "${LIMIT_MB}"

if [ "${bytes}" -ge $((LIMIT_MB * 1024 * 1024)) ]; then
  echo "FAIL: 超过 ${LIMIT_MB} MB 预算" >&2
  echo "  排查思路：go tool nm ${OUT} | sort | ... 看哪个依赖最大；控制面依赖里 sqlite/pgx 是主要占用。" >&2
  exit 1
fi
if [ "${bytes}" -ge $((WARN_MB * 1024 * 1024)) ]; then
  echo "WARN: 已接近上限（${mb} MB / ${LIMIT_MB} MB）"
fi
echo "OK"
