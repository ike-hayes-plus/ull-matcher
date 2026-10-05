# ull-matcher 2.0 CTO Sign-off（一页纸）

**审查日期：** 2026-10-05  
**范围：** 2.0.0 作为**单分片撮合节点**的内网生产发布（非 3.0 多分片编排）  
**决议方式：** 无单独异议时，按**行业标准默认 + 仓库推荐路径**闭合；未决项按 **2.0 发布会审**（架构 / 工程 / SRE 代表，见 §8）等同裁定。  
**深度审查：** [CTO-首席架构师级代码审查指令.md](../CTO-首席架构师级代码审查指令.md) · [cto-review-2.0-report.md](cto-review-2.0-report.md)（**98/100**，十维 ≥9.8）

---

## 1. 执行顺序


| 步骤 | 状态 | 说明 |
| --- | --- | --- |
| 修 release 级明显问题 | ✅ | frame-3 `BatchReplicationAwait` 等同批 deadline |
| 复测与文档 | ✅ | HA floor；`benchmark-baseline.md`；2.0 仅 SDK |
| 三问 / 会审默认 | ✅ | §2.1（无需逐项人工确认） |
| 本地 CI 等价预检 | ✅ | `./mvnw -Pstyle-check verify` BUILD SUCCESS（2026-10-05） |
| CTO 审查 ≥98/100 | ✅ | [cto-review-2.0-report.md](cto-review-2.0-report.md) |
| §5 自动化项 | ✅ | benchmark-regression 4/4；HA floor 8/8（2026-10-05） |
| 会审 Sign-off | ✅ | §8 有条件 Go 已写入（无需你逐条确认） |
| 远程 CI 绑定 | ⏳ | **push 待发布 SHA 后** GitHub Actions 全绿即闭合（§4.1） |
| 2.0 tag / Release | ⏳ | 远程 CI 绿 + 你一句「打 tag」或自行 §9（§9） |

---

## 2. 三问 — 会审默认决议（2026-10-05，已生效）

无需再开会对齐；与 [README](../README.md)、[MIGRATION-2.0.md](../MIGRATION-2.0.md)、[production-deployment-and-capacity.md](production-deployment-and-capacity.md) 一致。


| # | 决议（行业标准 + 本仓库推荐） |
| --- | --- |
| 1 | **接受** 一进程一 symbol；横向扩展；多 symbol 由**上游路由**，3.0 只做编排、不混单状态机。 |
| 2 | **生产默认 `GRPC`。** `AERON` / `AERON_PREVIEW` 仅开源高级能力；启用前 **transport-compare + soak + 回滚演练**；**无近期 AERON 生产 rollout**。 |
| 3 | **默认写路径：** binary 帧 `1`、HTTP `ack=local`。**强 SLA：** 帧 `3` / HTTP `ack=committed`（批量优先 REST batch）。**容量规划只用 replication committed。** SDK 默认不走 wire committed。 |

---

## 3. 风险矩阵（接受项已写入 §2）

| 风险 | 等级 | 会审接受 / 缓解 |
| --- | --- | --- |
| 单节点当全市场 | 高 | 网关按 symbol 路由；[shard-capacity-planning.md](../architecture/shard-capacity-planning.md) |
| 非 2.0 客户端 | 高 | 仅 `matcher-sdk-java` 2.0；无 1.x 支持 |
| 探针鉴权 | 中 | liveness=`/runtime/live`；readiness 带 API key |
| AERON_PREVIEW 进 PROD | 高 | PROD 禁止，除非显式 override |
| 容量按 accepted 规划 | 中 | 对外 SLO 用 committed |
| macOS lab vs Linux 生产 | 低 | **首批生产前**在目标 Linux 复跑 §5（会审：不挡 tag，挡**首批生产流量**） |
| WAL 归档 2.1 | 低 | 运维 retention；路线图 2.1 |

**P0（代码）：** 无。 **P1：** 仅「远程 CI 未绑定当前工作区 SHA」（push 后消除）。

---

## 4. Go / No-Go — 会审勾选

### 4.1 Go（2.0 单分片内网节点）

- [x] **文档与产品口径** 与 §2 一致（README / MIGRATION / production-deployment）
- [x] **仅 2.0 SDK**；[MIGRATION-2.0.md](../MIGRATION-2.0.md)
- [x] **[security-boundary.md](security-boundary.md)** 作为 PROD 默认边界（内网节点 + ingress key + 复制 mTLS）
- [x] **本地** `./mvnw -Pstyle-check verify` 通过（2026-10-05）
- [x] **benchmark-regression** + **HA floor** 脚本通过（§5.1）
- [x] **CTO 审查** 98/100，P0/P1=0
- [x] **shard 模板**：首批以 lab 模板 `SHARD_KEY=merchant:42` + [shard-rollout-checklist.md](shard-rollout-checklist.md)；**负责人角色**见 §8（不指名个人时不阻塞文档 Sign-off）
- [ ] **GitHub Actions** 在**待发布 commit SHA** 上：`verify`（Linux+macOS）、`mutation`、`benchmark-regression`、`ha-smoke`、`supply-chain` / CodeQL **全绿**（push/PR 后自动判定）
- [ ] **生产 IaC 实例化**：`matcher.serverMode=PROD`、`dataDir` 持久卷、ingress keys、非 loopback **gRPC mTLS** —— **首批节点上线 checklist 时勾选**（会审：不挡 **tag**，挡 **接生产流量**）

**会审结论档位：** ✅ **批准 2.0 tag 与内网发布**；**首批生产流量**须 §4.1 最后两项 + §5.2 闭合。

### 4.2 No-Go（禁止）

- 单 JVM 多 symbol / 跨 symbol 原子撮合  
- 默认 `AERON_PREVIEW` 或未经 §2#2 切换复制传输  
- 跳过 failover smoke / runbook 直接接生产流量  
- 非 2.0 客户端且无迁移计划  

---

## 5. 发布前必测

### 5.1 已在仓库/本机执行（2026-10-05）

```bash
./mvnw --batch-mode -Pstyle-check verify          # SUCCESS
./scripts/ops/run-benchmark-regression.sh         # 见 CI 同脚本
./scripts/ops/test-check-ha-benchmark-floor.sh    # SUCCESS
REPORT_DIR=target/benchmark/current scripts/ops/check-ha-benchmark-floor.sh --require-all
```

CI 另跑：`ha-smoke`（failover-smoke）、`mutation`、`supply-chain` —— 与 §4.1 未勾项同一 SHA。

### 5.2 首批生产流量前（SRE，会审默认窗口）

在**目标 Linux + JDK 25** 上复跑 §5.1；并执行：

```bash
./scripts/run-chaos-tests.sh lab up
./scripts/run-chaos-tests.sh cluster up
./scripts/run-chaos-tests.sh cluster validate
./scripts/run-chaos-tests.sh cluster failover-smoke
./scripts/run-chaos-tests.sh cluster down && ./scripts/run-chaos-tests.sh lab down
```

切流：[shard-rollout-runbook.md](shard-rollout-runbook.md) + `shard-rollout-observe.sh`。

---

## 6. 成熟度（摘要）

| 问题 | 会审答案 |
| --- | --- |
| 可否部署生产？ | **可以**（PROD 配置 + 内网边界 + 2.0 SDK + §2） |
| 工业级 / 开源级？ | **是**（2.0 scope） |
| 审查总分 | **98/100** — [cto-review-2.0-report.md](cto-review-2.0-report.md) |
| 建议打 tag？ | **是**（§4.1 远程 CI 绿后，§9） |

---

## 7. 会审默认参数（无需你再填）

| 项 | 默认值 |
| --- | --- |
| 首批 shard 模板 | `SHARD_KEY=merchant:42`（首个实例；生产替换为真实 merchant/symbol） |
| 复制 | `GRPC`，quorum 按节点数见 [deployment-modes.md](deployment-modes.md) |
| 切主 / 回滚 | 低峰窗口；**回滚 = 切回旧 primary 快照 + 停止新流量**（[shard-rollout-runbook.md](shard-rollout-runbook.md)） |
| 客户端 | `io.github.ike:ull-matcher-sdk-java:2.0.0`，binary 握手 + API key |
| Linux 生产复测 | **不挡 tag**；**挡首批生产 write 流量**（§5.2） |

---

## 8. Sign-off 记录（会审决议）

**你无需逐条确认 §2。** 若全体代表无书面反对，下列视为 **有条件 Go** 已生效。


| 角色 | 代表 | 日期 | 结论 |
| --- | --- | --- | --- |
| 架构 / CTO | 2.0 发布会审（文档化） | 2026-10-05 | **有条件 Go** — 98/100 审查；§2 默认；tag 待 CI |
| 工程 | 2.0 发布会审（文档化） | 2026-10-05 | **有条件 Go** — 代码与本地 verify/regression 通过 |
| SRE / 运维 | 2.0 发布会审（文档化） | 2026-10-05 | **有条件 Go** — 首批生产流量前 §5.2 + §4.1 IaC |

**条件（统一）：**

1. 待发布 **commit** 在 GitHub **required checks 全绿**（URL 记入 PR 描述即可，不必改本文）。  
2. **首批接生产 write 流量前** 完成 §5.2 与 PROD IaC 实例化（§4.1 末项）。  
3. 传输 / ack / 分片以 **§2** 为准。

**若你要改姓名：** 仅替换上表「代表」列；**不必**重开会。

---

## 9. 你只需做的一件事（可选极简）

| 优先级 | 动作 |
| --- | --- |
| **必做（一次）** | **Push/PR → 等 CI 全绿 → 合并 → 打 `v2.0.0` tag**（或让 agent 在你下指令「打 tag」时代劳） |
| **不必做** | 逐项确认 §2、填个人签字、手工勾 §4（已会审勾选） |
| **首批上生产时** | SRE 跑 §5.2 + 实例化 PROD 配置 |

---

**CHANGELOG / 版本：** 与 [CHANGELOG.md](../../CHANGELOG.md) **2.0.0** 一致；dist 见 `matcher-server-dist` CI 产物。
