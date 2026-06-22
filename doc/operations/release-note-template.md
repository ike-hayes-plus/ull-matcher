# 发布说明模板

本文档用于正式开源发布、预发布版本发布、或生产版本发布时复用。

建议每次发布都基于这份模板生成一份独立说明。

---

## 1. 版本信息

- 版本号：
- 发布时间：
- 发布类型：
  - 正式发布
  - 预发布
  - 补丁发布
- 适用范围：
  - 开源用户
  - 生产部署
  - 内部验证

## 2. 本次发布摘要

一句话摘要：

> 例如：本次版本收紧了 binary ingress + HA committed 主链，默认生产建议继续使用 `GRPC`，并补齐了灰度发布、容量规划和稳定性证据。

## 3. 主要变更

### 3.1 新增能力

- 
- 
- 

### 3.2 性能与吞吐改进

- 
- 
- 

### 3.3 高可用与稳定性改进

- 
- 
- 

### 3.4 文档与脚本改进

- 
- 
- 

## 4. 不兼容变更

如果没有，明确写：

- 无

如果有，逐项写清：

- 变更点：
- 影响对象：
- 迁移方式：

## 5. 默认值与推荐口径

当前推荐：

- 高频主入口：`binary ingress`
- 通用入口：`REST`
- 默认复制传输：`GRPC`
- 可选高性能传输：`AERON`
说明：

- 默认值表示当前保守生产建议，不表示所有拓扑都绝对最快。

## 6. 基准与容量口径

发布说明里必须明确 benchmark 口径。

建议至少写清：

- 是否包含 network
- 是否包含 IPC
- 是否包含 WAL
- 是否包含 HA committed
- 是否为 single-node / `1P1S` / `1P2S` / `1P3S`
- 是否为 `REST` / `binary ingress`

建议引用：

- [benchmark-baseline-current.md](benchmark-baseline-current.md)
- [production-deployment-and-capacity.md](production-deployment-and-capacity.md)
- [shard-capacity-planning.md](../architecture/shard-capacity-planning.md)

## 7. 本次发布使用的稳定事实源

示例写法：

- Single-node binary:
  - accepted:
  - p99:
- External `1P1S` binary + `GRPC` replication committed:
  - accepted:
  - committed:
  - catch-up:
  - p99:

注意：

- 只写 current 稳定事实源
- 不引用一次性 rerun / debug 结果

## 8. 灰度与发布建议

推荐发布方式：

- 按 shard 灰度
- 先升 standby，再切 primary
- 先灰度查询与控制面，再灰度高频写流量

参考：

- [gray-release-runbook.md](gray-release-runbook.md)
- [gray-release-checklist.md](gray-release-checklist.md)

## 9. 回滚条件

至少明确以下回滚条件：

- `replicationCommittedSequence` 推进明显变慢
- `standbyApplyQueueDepth` 持续堆积
- `clientTrafficReady` 与真实主节点不一致
- `p99` 或 committed throughput 明显恶化
- failover 后恢复不完整

## 10. 发布前检查清单

发布前建议确认：

- [ ] 全量测试已通过
- [ ] current benchmark 已更新
- [ ] 默认值说明已核对
- [ ] 灰度 runbook 已核对
- [ ] 回滚方案已准备
- [ ] 文档索引已更新

## 11. 已知限制

建议明确写出当前仍然存在的边界，例如：

- 当前默认值是保守生产建议，不代表所有拓扑下绝对最快
- `AERON` 仍建议在目标拓扑下单独完成 soak / chaos 验证
- 单分片吞吐不等于整个平台总吞吐

## 12. 相关文档

- [README.md](../../README.md)
- [production-deployment-and-capacity.md](production-deployment-and-capacity.md)
- [benchmark-baseline-current.md](benchmark-baseline-current.md)
- [gray-release-runbook.md](gray-release-runbook.md)
- [gray-release-checklist.md](gray-release-checklist.md)
- [shard-capacity-planning.md](../architecture/shard-capacity-planning.md)
