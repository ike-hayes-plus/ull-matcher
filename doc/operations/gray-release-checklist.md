# Shard Rollout 检查清单

本文档用于按 shard 切换流量时逐项勾选，避免遗漏关键检查点。

## 1. 切换前

- [ ] 已确认目标构建与生产数据格式兼容
- [ ] 基线 benchmark 验证通过
- [ ] `soak / failover smoke / chaos` 验证通过
- [ ] 已明确 rollout 的 shard 范围
- [ ] 已明确使用的复制传输：`GRPC` 或 `AERON`
- [ ] 已确认生产一次只启用一种权威复制传输
- [ ] 已准备好回滚节点
- [ ] 已确认日志目录、数据目录、证书目录可用
- [ ] 已确认监控、告警、采样脚本可用

## 2. 切流前

- [ ] 目标节点作为 standby 加入
- [ ] `serviceReady=true`
- [ ] `clientTrafficReady=false`
- [ ] `lastAppliedSequence` 追近 primary
- [ ] `lastDurableSequence` 持续推进
- [ ] `standbyApplyQueueDepth` 未持续堆积
- [ ] `standbyAckLastFlushMicros` 未出现异常尖峰
- [ ] 执行切主前采样：

```bash
./scripts/deploy/gray-release-observe.sh target/gray/before \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8081
```

## 3. 切主窗口内

- [ ] 在低峰窗口执行
- [ ] 明确人工切主负责人
- [ ] 明确回滚负责人
- [ ] 执行切主或下线原 primary
- [ ] 新 primary 出现
- [ ] 未出现双主

## 4. 切主后

- [ ] 执行切主后采样：

```bash
./scripts/deploy/gray-release-observe.sh target/gray/after \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8081
```

- [ ] 恰好一个 `PRIMARY`
- [ ] 恰好一个 `clientTrafficReady=true`
- [ ] `PRIMARY` 与 `clientTrafficReady` 节点一致
- [ ] `replicationCommittedSequence` 持续推进
- [ ] `replicationQueueDepth` 未异常堆积
- [ ] `replicationLastCommitMicros` 未明显恶化
- [ ] `accepted -> committed` 折损未明显恶化
- [ ] `p99` 未明显恶化

## 5. 扩大灰度范围前

- [ ] 目标 shard 稳定通过观察窗口
- [ ] 没有出现 committed tail 明显拉长
- [ ] 没有出现 standby apply backlog 持续堆积
- [ ] 没有出现 readiness 与真实主节点状态不一致
- [ ] 决定后续 shard 范围

## 6. 回滚条件

满足以下任一项，停止放量并准备回滚：

- [ ] `replicationCommittedSequence` 推进明显变慢
- [ ] `standbyApplyQueueDepth` 持续堆积
- [ ] `clientTrafficReady` 与真实主节点不一致
- [ ] failover 后恢复不完整
- [ ] `p99` 或 committed throughput 明显恶化

## 7. 回滚后

- [ ] 执行回滚后采样：

```bash
./scripts/deploy/gray-release-observe.sh target/gray/rollback \
  http://127.0.0.1:8080 \
  http://127.0.0.1:8081
```

- [ ] 回滚节点重新成为唯一 `PRIMARY`
- [ ] `clientTrafficReady` 与唯一 `PRIMARY` 一致
- [ ] `replicationCommittedSequence` 恢复正常推进
- [ ] 停止扩大灰度范围
