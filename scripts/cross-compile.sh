#!/usr/bin/env bash
# 三平台交叉编译（linux/amd64、linux/arm64、darwin/arm64），CI 里约 80s。
#
# 为什么是脚本而不是两处 YAML 里的同一段：`ci.yml` 的 cross-compile job 与 `release.yml`
# 的 cross-compile job 必须验**同一组**平台。以前靠「逐字相同 + 注释提醒同步」，
# 那是人肉前提——两边漂移的后果是「验的平台不一样，而且两边都绿，没人发现」，比没验更难查。
# 现在真源只有这一个文件；改平台列表改这里。
set -euo pipefail

for target in linux/amd64 linux/arm64 darwin/arm64; do
  echo "building $target"
  CGO_ENABLED=0 GOOS=${target%/*} GOARCH=${target#*/} go build -trimpath -o /dev/null ./cmd/jinliang
done
