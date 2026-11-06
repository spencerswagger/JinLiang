#!/usr/bin/env bash
# 单二进制体积守门：docs/BUILD.md §1 说发布产物要 <25 MB。
#
# 两个模式：
#   scripts/check_binary_size.sh [OUT]            自己用**发布参数**构建再量（CI 用）
#   scripts/check_binary_size.sh --from-file PATH  量一个现成产物，并验证它确实是发布构建
#
# 为什么验证「是不是发布构建」：真机上报过 29.4 MB 超标，实际是 `go build` 的
# debug 产物（带 DWARF + 符号表）；同一天同一提交的发布构建只有 20.1 MB。
# 不验证参数就会把「构建方式不对」误判成「预算不够」。
set -euo pipefail

LIMIT_MB="${LIMIT_MB:-25}"
WARN_MB="${WARN_MB:-22}"
OUT="dist/jinliang"
FROM_FILE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --from-file) shift; FROM_FILE="$1" ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
    *) OUT="$1" ;;
  esac
  shift
done

# 发布产物的三个硬条件：静态（CGO=0）、可复现路径（-trimpath）、无调试信息（-s -w）。
verify_release_binary() {
  local f="$1"
  if go tool nm "$f" >/dev/null 2>&1; then
    echo "FAIL: $f 带符号表——这是 debug 构建，不是发布产物（-s -w 没生效）" >&2
    echo "  典型差异：debug 约 29.4 MB，同提交发布构建约 20.1 MB；差的 ~9 MB 是 DWARF + symtab。" >&2
    return 1
  fi
  local info
  info=$(go version -m "$f" 2>/dev/null || true)
  if ! printf '%s' "$info" | grep -q 'trimpath=true'; then
    echo "FAIL: $f 不是 -trimpath 构建（发布产物要求可复现路径）" >&2
    return 1
  fi
  if ! printf '%s' "$info" | grep -q 'CGO_ENABLED=0'; then
    echo "FAIL: $f 不是 CGO_ENABLED=0 的静态产物" >&2
    return 1
  fi
}

if [ -n "$FROM_FILE" ]; then
  OUT="$FROM_FILE"
  [ -f "$OUT" ] || { echo "check_binary_size: 找不到 $OUT" >&2; exit 2; }
  verify_release_binary "$OUT" || exit 1
else
  mkdir -p "$(dirname "${OUT}")"
  echo "构建参数: CGO_ENABLED=0 go build -trimpath -ldflags \"-s -w\" -o ${OUT} ./cmd/jinliang"
  CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=sizecheck" -o "${OUT}" ./cmd/jinliang
  verify_release_binary "$OUT" || exit 1
fi

bytes=$(wc -c <"${OUT}")
mb=$((bytes / 1024 / 1024))
printf 'binary %s: %s bytes (%s MB), limit %s MB\n' "${OUT}" "${bytes}" "${mb}" "${LIMIT_MB}"

if [ "${bytes}" -ge $((LIMIT_MB * 1024 * 1024)) ]; then
  echo "FAIL: 超过 ${LIMIT_MB} MB 预算" >&2
  echo "  排查思路：go tool nm -size 在 -w（未 -s）构建上看符号；主要占用通常是 modernc.org/sqlite、pgx/pgtype、crypto/tls。" >&2
  exit 1
fi
if [ "${bytes}" -ge $((WARN_MB * 1024 * 1024)) ]; then
  echo "WARN: 已接近上限（${mb} MB / ${LIMIT_MB} MB）"
fi
echo "OK"
