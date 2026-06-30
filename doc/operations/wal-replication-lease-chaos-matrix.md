# WAL / 复制 / 租约混沌测试方案与执行矩阵

## 目标

验证以下三个面在异常条件下的行为：

1. WAL 持久化与恢复
2. 主备复制与追平
3. 租约、fencing 与自动接管

## 测试前提

- 至少 2 个 matcher 节点
- 1 套 ZooKeeper
- 可选 1 套三成员 etcd quorum
- 独立数据目录
- 可观察项：
  - HTTP readiness
  - `/metrics`
  - WAL 目录
  - ZooKeeper lease 路径
- gRPC 复制日志

## 自动化执行入口

项目把一部分可在单机进程内稳定复现的混沌场景收成 `@Tag("chaos")` 测试，可直接执行：

```bash
mvn -Pchaos-tests test
```

或：

```bash
./scripts/run-chaos-tests.sh
```

环境级场景使用统一脚本入口：

```bash
./scripts/run-chaos-tests.sh env help
```

场景汇总与自动结论输出：

```bash
./scripts/run-chaos-tests.sh summarize target/chaos-lab
./scripts/run-chaos-tests.sh summarize target/failover-smoke
```

容器化实验环境入口：

```bash
./scripts/chaos/lab.sh up
./scripts/chaos/lab.sh down
./scripts/chaos/lab.sh validate
```

例如：

```bash
DRY_RUN=true ./scripts/run-chaos-tests.sh env zk-disconnect
DATA_DIR=/data/ull-matcher ./scripts/run-chaos-tests.sh env wal-disk-fill
PRIMARY_PID=12345 ./scripts/run-chaos-tests.sh env kill-primary
./scripts/run-chaos-tests.sh collect out/node-a http://127.0.0.1:8080
./scripts/run-chaos-tests.sh validate out/node-a out/node-b out/node-c
```

`lab up` 会拉起：

- ZooKeeper
- etcd
- Toxiproxy
- Prometheus

其中 matcher 节点仍建议直接运行在宿主机，用于保留绑核、NUMA、本地 NVMe 等高性能部署特性。

完整 HA / 分片实验手册见：

- [`ha-sharding-lab.md`](ha-sharding-lab.md)

这些自动化用例覆盖的是“可重复、可断言、无需外部系统干预”的场景。涉及磁盘打满、`tc netem`、真实 ZooKeeper/etcd 网络分区的场景仍然保留为运维级手工/集成演练。

## 关注指标

- 下单 RT / p99
- `gateway.walForceCount`
- `lastAppliedSequence`
- `replication lag`
- `promotionReady`
- `snapshotSyncRequired`
- `tickFailureCount`
- `fencing epoch`

## 执行矩阵

| 编号 | 场景 | 注入方式 | 预期结果 | 失败信号 |
| --- | --- | --- | --- | --- |
| C1 | WAL fsync 抖动 | `fio` / `tc` / 设备限速 | 提交 RT 上升，但服务不乱序 | `ACCEPTED` 丢单、序列回退 |
| C2 | WAL 磁盘满 | 填满 data 盘 | 新命令提交失败，节点停止接单 | 仍返回成功、恢复后状态错误 |
| C3 | WAL 尾部损坏 | 截断最后一个 segment 尾部 | 恢复停在最后有效记录 | 恢复崩溃或读脏数据 |
| C4 | 快照存在 + WAL 缺段 | 删除部分旧 segment | 恢复失败并明确报错 | 静默启动但订单簿错误 |
| C5 | standby apply 变慢 | 人工阻塞 standby loop | replication timeout，readiness 降级 | primary 卡死、无限等待 |
| C6 | gRPC 网络抖动 | `tc netem delay/loss` | lag 上升但不双主 | readiness 错误放行 |
| C7 | primary 与 ZooKeeper 断联 | 切断 ZK 网络 | primary 自我 fencing | 断联后仍继续接单 |
| C8 | standby 与 ZooKeeper 断联 | 切断 standby 到 ZK | standby 不误 promote | 出现双主 |
| C9 | primary 进程 kill -9 | 直接杀进程 | standby 接管，重启后 snapshot + replay | 接管失败或恢复回退 |
| C10 | discovery 地址漂移 | 改实例地址 / 端口 | client 重建连接 | 仍访问旧 endpoint |

## 自动化覆盖

| 编号 | 自动化状态 | 对应用例 |
| --- | --- | --- |
| C3 | 已自动化 | `SegmentedMmapWalTest#ignoresAndOverwritesPartialTailRecord` |
| C4 | 已自动化 | `SegmentedMmapWalTest#manifestValidationFailsWhenReferencedSegmentIsMissing` |
| C5 | 已自动化 | `StandbySyncServiceTest#replicateTimesOutWhenApplyCannotMakeProgress` |
| C7 | 已自动化 | `HaCoordinatorTest#primaryFencesItselfAfterLeaseLoss` |
| C10 | 已自动化 | `DiscoveryDrivenReplicatorTest#refreshRebuildsTargetWhenEndpointChanges` |

其余场景需要在真实环境执行与观测，因为它们依赖：

- 真实磁盘抖动或磁盘打满
- 真实 ZooKeeper / etcd 网络分区
- 跨节点流量控制与 `tc netem`
- 外部进程 `kill -9`

## 观测采集与状态校验

采集与校验脚本：

- `./scripts/run-chaos-tests.sh collect <out-dir> <base-url>`
- `./scripts/run-chaos-tests.sh validate <node-dir> [<node-dir> ...]`

用途：

- `collect`：抓取节点 `health / readiness / metrics`
- `validate`：校验多节点场景下：
  - 最多只有一个 `PRIMARY`
  - 最多只有一个 `clientTrafficReady=true`
  - `PRIMARY` 与可接业务流量节点一致
- `summarize`：汇总 `validation-report.json`、`failover-smoke-report.json` 等场景结果，并输出统一 `conclusion / severity / successCount`

## 推荐执行顺序

1. 单机 WAL 场景：`C1` ~ `C4`
2. 主备复制场景：`C5`、`C6`
3. 租约 / fencing 场景：`C7` ~ `C9`
4. 发现层场景：`C10`

## 判定标准

### 通过

- 没有双主
- 没有序列回退
- 恢复后订单簿与 WAL 一致
- readiness 与真实状态一致

### 不通过

- 返回 `ACCEPTED` 但恢复后命令丢失
- fencing 后节点仍继续接单
- standby 未追平却被 promote
- endpoint 漂移后复制仍打旧地址

## WAL durability mode 专项建议

### `SYNC_PER_COMMAND`

- 重点观察提交 RT、磁盘抖动敏感度

### `SYNC_PER_BATCH`

- 重点观察单批丢失窗口是否符合预期
- 建议记录：
  - batch size
  - force 前累计命令数

### `OS_BUFFERED`

- 只建议在吞吐优先、允许尚未刷盘命令丢失的场景验证
- 重点检查：
  - 宕机后最多丢多少命令
  - 上游是否能接受该 durability 语义
