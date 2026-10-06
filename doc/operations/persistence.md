# 持久化预设（RDB + AOF）

统一入口 `matcher.persistenceProfile` 将 Redis 风格的 **RDB**（周期快照）与 **AOF**（WAL 耐久性）映射到现有 WAL/快照格式，不改变磁盘布局与撮合顺序。

## 预设

| 预设 | WAL 耐久 | batch / delay | 周期快照 | 说明 |
|------|----------|---------------|----------|------|
| `PROD` | `SYNC_PER_COMMAND` | 1 / 0 | 60s | `matcher.serverMode=PROD` 时须配置 `matcher.walColdArchiveDir` 且 `snapshotIntervalMillis > 0` |
| `BENCH` | `SYNC_PER_BATCH` | 32 / 1s | 关 | 吞吐压测；禁止与 `matcher.serverMode=PROD` 同用 |
| `LAB` | `OS_BUFFERED` | 1 / 0 | 关 | 禁止与 `matcher.serverMode=PROD` 同用 |
| *(未设)* | DEV 默认 | 1 / 0 | 关 | 非 PROD 且未指定预设 |

`matcher.serverMode=PROD` 且未显式指定预设时，自动套用 **`PROD`**。

## 属性

| 属性 | 用途 |
|------|------|
| `matcher.persistenceProfile` | `PROD`、`BENCH`、`LAB` |
| `matcher.snapshotIntervalMillis` | 后台周期快照间隔；`0` 表示关闭 |
| `matcher.walDurabilityMode` | 覆盖预设中的 WAL 模式 |
| `matcher.walForceBatchSize` | 覆盖 batch fsync 大小 |
| `matcher.walForceMaxDelayMicros` | 覆盖 batch 最大等待 |
| `matcher.walColdArchiveDir` | WAL 冷备目录（PROD 必填） |

Spring Boot：`ull.matcher.persistence-profile`、`ull.matcher.snapshot-interval-millis`、`ull.matcher.wal-cold-archive-dir`；仅当 YAML 显式配置时才覆盖预设中的 WAL / 快照字段。

## 运行行为

- 手动快照：`POST /api/v1/admin/snapshot`。
- 周期快照：单线程 daemon、`scheduleWithFixedDelay`（上一拍结束后再等间隔）。
- **HA 集群**：仅 **PRIMARY** 可创建或导出权威快照（周期 RDB、手动 admin 快照、gRPC/Aeron `latestSnapshot`）。备库调用返回错误，避免未追平状态被当成 RDB 源。
- **权威快照安装**：`installSnapshotFrom` 与 fenced rejoin 一样先隔离本地 WAL，再从权威快照重建；启动时若快照存在且本地仍有 WAL 分段，必须有匹配的 checkpoint manifest。
- 冷备目录与保留策略：[wal-archive-cold-backup.md](wal-archive-cold-backup.md)。

## 独立 JVM（生产示例）

```bash
java ... \
  -Dmatcher.serverMode=PROD \
  -Dmatcher.persistenceProfile=PROD \
  -Dmatcher.dataDir=/var/lib/ull-matcher/node-a \
  -Dmatcher.walColdArchiveDir=/var/lib/ull-matcher/wal-cold \
  io.github.ike.ullmatcher.server.bootstrap.MatcherServerMain
```

`scripts/deploy/` 在 `SERVER_MODE_VALUE=PROD` 时设置冷备路径与 `PERSISTENCE_PROFILE`；未设 `SNAPSHOT_INTERVAL_MILLIS` 时由 `PROD` 预设提供 60s。未显式设置 `WAL_COLD_ARCHIVE_DIR` 时，脚本默认用 `$dataDir/wal-cold`（与热盘同卷，只满足启动闸门，不是跨盘灾备）。

## Lab：全链路观察

本地对比 **BENCH vs PROD** WAL（`SERVER_MODE_VALUE=DEV`，不强制冷备）：

```bash
scripts/lab/run-persistence-full-chain-sweep.sh
```

Lab 节点经 `start-node.sh` 默认 `BENCH`、关闭周期 RDB；调用方已导出的 `PERSISTENCE_PROFILE` / `SNAPSHOT_INTERVAL_MILLIS` / `WAL_*` 不被覆盖。

路径：Embedded → Binary 单节点 → HA binary replication committed。报告默认 `target/persistence-full-chain-sweep/`。

正式容量基线仍用 [benchmark-baseline.md](benchmark-baseline.md) 与 `scripts/ops/run-ha-benchmark-suite.sh`（32768 窗口）。
