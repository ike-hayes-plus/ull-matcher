#!/usr/bin/env bash
# 为已推送的 git tag 创建 GitHub Release（需本机 gh auth login）。
set -euo pipefail

repo="${1:-ike-hayes-plus/ull-matcher}"

if ! command -v gh >/dev/null 2>&1; then
  echo "install gh: brew install gh && gh auth login" >&2
  exit 1
fi

gh release view v1.1.0.0 --repo "$repo" >/dev/null 2>&1 || \
  gh release create v1.1.0.0 --repo "$repo" --title "v1.1.0.0" --notes "$(cat <<'EOF'
首个公开基线（2026-07-02，`b3015cc`）。

- 初始开源撮合引擎与 HA 骨架
- 详见仓库该 tag 时间点文档

EOF
)"

gh release view v2.0.0 --repo "$repo" >/dev/null 2>&1 || \
  gh release create v2.0.0 --repo "$repo" --title "v2.0.0" --notes "$(cat <<'EOF'
**2.0 单分片生产基线**（`8196f6c`，2026-10-05）

- JDK 25 / Maven 4，`./mvnw`
- 生产默认 gRPC 复制、ingress 鉴权、etcd mTLS、PROD 安全闸门
- Java SDK 3.0；见 [INTEGRATION.md](../INTEGRATION.md)
- CTO sign-off：[cto-signoff-3.0.md](cto-signoff-3.0.md)

**不含** 3.0 `matcher-orchestrator`（在 `v2.0.0` 之后的 master 提交）。

EOF
)"

echo "Done. Open: https://github.com/${repo}/releases"
