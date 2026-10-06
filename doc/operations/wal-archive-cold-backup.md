# WAL 分段归档与冷备

## 行为

- 热 WAL 仍写在 `{dataDir}/wal/`，由 `SegmentedMmapWal` 分段滚动。
- **快照成功后**，引擎会删除已被快照覆盖、且非当前写入段的 WAL 文件。
- 若配置了冷备目录，删除前会把分段 **复制** 到冷备路径并 `fsync`，复制失败则 **保留** 热 WAL（与 `WalSegmentArchiver` 契约一致）。

## 配置

| 入口 | 属性 | 说明 |
| --- | --- | --- |
| 独立进程 | `matcher.walColdArchiveDir` | 绝对或相对路径；未设置则不归档 |
| Spring Boot | `ull.matcher.wal-cold-archive-dir` | 同上 |

冷备布局：

```text
{walColdArchiveDir}/{shardKey}/{nodeId}/{segmentFileName}
```

`shardKey` / `nodeId` 中的 `/`、`\` 会替换为 `_`，避免路径注入。

## PROD 闸门

`MatcherServerConfig.validateDeploymentSafety()` 在 **PROD** 下要求：

- 冷备目录不得落在 `target` / `build` / `out` 等构建临时路径；
- 冷备目录 **不得** 与热 `walDirectory` 相同。

## 运维建议

1. 冷备卷与 `dataDir` 使用 **不同挂载**（NFS / 对象存储网关 / 独立磁盘）。
2. 快照周期应小于热盘容量规划；归档段仅含 **已被快照覆盖** 的历史，恢复时仍需 **最新快照 + 热 WAL 尾段**。
3. 对象存储（S3/OSS）可在冷备目录上挂载同步工具，或实现自定义 `WalSegmentArchiver`（SDK 不绑定云厂商客户端）。
4. 保留策略：冷备侧按日期/lifecycle 删除；与 [wal-replication-lease-chaos-matrix.md](wal-replication-lease-chaos-matrix.md) 中的 DR 演练一并验证。

## 验证

```bash
# 单元测试
./mvnw -pl matcher-storage test -Dtest=DirectoryWalSegmentArchiverTest,SegmentedMmapWalTest

# 启用冷备启动（lab）
-Dmatcher.walColdArchiveDir=/var/lib/ull-matcher/wal-cold
```
