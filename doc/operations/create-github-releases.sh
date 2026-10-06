#!/usr/bin/env bash
# 为已推送的 git tag 创建 GitHub Release（需本机 gh auth login）。
set -euo pipefail

repo="${1:-ike-hayes-plus/ull-matcher}"
tag="${2:-v3.0.0}"

if ! command -v gh >/dev/null 2>&1; then
  echo "install gh: brew install gh && gh auth login" >&2
  exit 1
fi

if gh release view "$tag" --repo "$repo" >/dev/null 2>&1; then
  echo "Release $tag already exists on $repo"
  exit 0
fi

gh release create "$tag" --repo "$repo" --title "$tag" --notes "$(cat <<'EOF'
**3.0** 单分片节点 + 可选 etcd 多分片编排。

- JDK 25 / Maven 4，`./mvnw -Pstyle-check verify`
- 生产默认 gRPC 复制、HTTP/binary ingress 鉴权、PROD 安全闸门
- SDK：`io.github.ike:ull-matcher-sdk-java:3.0.0`
- 部署：[production-deployment-and-capacity.md](production-deployment-and-capacity.md)

EOF
)"

echo "Done. Open: https://github.com/${repo}/releases/tag/${tag}"
