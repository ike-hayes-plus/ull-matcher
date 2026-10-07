# 参与贡献

## 作用域

本仓库聚焦于撮合内核、撮合节点服务与配套 Java SDK。提交修改时，请保持在以下边界内：

- 撮合核心
- WAL、快照、重放
- runtime loop 与 gateway
- HA 控制面与复制链
- 独立 matcher 节点服务
- Java SDK 接入封装

不要把账户账本、清结算、行情分发或无关业务流程混入本仓库。

## 环境要求

必需：

- JDK 25
- Maven 4，使用仓库里的 `./mvnw`（wrapper 钉死 `4.0.0-rc-7`）

推荐初始化方式：

```bash
sdk install java 25.0.3-tem
sdk env install
sdk env
```

仓库已提交 `.sdkmanrc`。请使用 `sdk env` 选择的项目本地 JDK，不要依赖 shell 的全局默认 Java。

当使用 `matcher.replicationTransport=AERON` 时，需要补充：

```bash
--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

复制传输类型跨重启切换受持久化锁保护，必须显式指定变更窗口：

```bash
-Dmatcher.allowTransportChange=true
-Dmatcher.transportChangeWindowId=<ticket-or-window-id>
```

## 构建与测试

提交 Pull Request 前运行完整 verify，它会跑样式检查、全部模块的测试和覆盖率门禁：

```bash
./mvnw -Pstyle-check verify
```

新 Java 文件请在 `package` 声明前加 SPDX 头：

```
/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
```

覆盖率门禁对挂了 `jacoco-maven-plugin` 的模块统一要求 line ≥ 0.80、branch ≥ 0.70。
`matcher-examples`、`matcher-benchmarks`、`matcher-server-dist` 不跑 JaCoCo。
父 POM 排除 gRPC/Aeron 传输宿主、`MatcherClusterSupervisor` 和 `MatcherServerMain`（由 HA smoke / 启动测试覆盖）。
生成的报告在 `<module>/target/site/jacoco/index.html`。

> 如果你新增了一个模块的 surefire `<argLine>` 覆盖配置，必须保留 `@{jacocoArgLine}`，
> 否则 JaCoCo agent 会被静默丢弃，该模块的覆盖率门禁会变成空操作。

撮合内核与 runtime 还有变异测试门禁（mutation score ≥ 70）。它比普通测试慢得多，
所以单独放在一个 profile 里，CI 会跑，本地按需：

```bash
./mvnw -Pmutation -pl matcher-core,matcher-runtime -am verify
```

热路径的性能回归门禁（不需要集群，CI 每次都跑）：

```bash
./scripts/ops/run-benchmark-regression.sh
```

供应链检查：

```bash
./mvnw -Psbom package -DskipTests                 # CycloneDX SBOM
./mvnw -Pdependency-check verify -DskipTests      # OWASP 已知漏洞扫描
```

混沌测试由 CI 的 `verify` job 额外跑 `-Pchaos-tests`。本地复现：

```bash
./mvnw test -Pchaos-tests
```

签名发布到中央仓库的路径：

```bash
./mvnw -Pstyle-check,release-signing,central-publish -DskipTests deploy
```

## 代码约束

- 正确性优先于局部性能改动。订单顺序、成交规则、撤单语义、幂等语义、WAL/replay 恢复语义不能为吞吐数字让路。
- 撮合热路径必须简单、可控，避免锁竞争、无谓分配、反射、字符串解析和日志拼接。
- 新增服务层策略必须经过标准命令、WAL、复制和重放主链，不能绕开可恢复状态。
- 不要把 discovery、TTL、TLS、集群协调或网络协议逻辑塞进 `matcher-core`。
- 优先沿用现有模块边界与模式，不随意引入新抽象。
- 测试要聚焦真实行为风险，避免低信号的大而全测试。
- 性能相关改动必须能解释 benchmark 口径，至少说明是否包含网络、WAL、HA committed、外部进程和复制确认。
- 遵循 `.editorconfig` 的缩进与空白约定。
- Java 代码避免通配符导入。
- 源文件中不要引入 tab。

## 模块边界

- `matcher-core`：纯撮合引擎
- `matcher-storage`：WAL、快照、重放
- `matcher-runtime`：loop 与 gateway
- `matcher-ha`：HA 契约与编排
- `matcher-ha-zookeeper`：ZooKeeper lease / fencing 实现
- `matcher-ha-etcd`：etcd lease / discovery 实现
- `matcher-discovery-zookeeper`：ZooKeeper discovery 实现
- `matcher-ha-grpc`：gRPC 复制传输
- `matcher-ha-aeron`：Aeron 复制传输
- `matcher-server`：独立撮合节点服务
- `matcher-sdk-java`：Java SDK，封装 HTTP API 与 binary ingress 客户端
- `matcher-spring-boot-starter`：Spring Boot 集成入口
- `matcher-examples`：benchmark、验证和示例入口

## 测试与性能基线

- 修改撮合、WAL、复制、replay、幂等或 API 契约时，必须补充或更新对应单元测试。
- 修改热路径时，优先增加低分配、低抖动的专向 benchmark，而不是只看端到端脚本单次读数。
- benchmark 报告应写入 `target/` 下的生成目录；仓库文档只保留稳定事实源和可复现命令。
- 不把单次最好结果写成容量承诺；README 使用保守稳定口径，完整事实源维护在 `doc/operations/benchmark-baseline.md`。
- 压测失败时先判断环境、JDK、端口、ZooKeeper、standby catch-up 和 committed 口径，再判断业务代码退化。

## 文档约束

- `README.md` 只作为入口和推荐口径，不沉淀长篇验证过程。
- 架构设计放在 `doc/architecture/`。
- 运维、压测、发布和容量文档放在 `doc/operations/`。
- 贡献流程、代码边界、测试和发布要求统一放在本文件。
- 生成报告不要提交到仓库；运行产物统一写入 `target/`。

## Pull Request 要求

- Pull Request 保持聚焦，不要同时夹带无关改动。
- 当用户可见行为、API 或配置变更时，必须同步更新 `README.md`。
- 任何用户可见的变更都要在 `CHANGELOG.md` 的 `Unreleased` 段落里记一笔；
  破坏性变更标 `BREAKING`。3.0 是绿田基线，不维护历史版本迁移路径。
- 如果修改影响发布产物或公开元数据，请确认 `.github/workflows/release.yml` 仍与预期产物集合一致。

## 发布说明

发布流程支持三层：

1. Git tag 与 GitHub Release 产物发布
2. 可选的 GitHub Packages Maven 发布
3. 可选的 Maven Central 发布

发布到 Maven Central 需要以下 secret：

- `MAVEN_CENTRAL_USERNAME`
- `MAVEN_CENTRAL_PASSWORD`
- `MAVEN_GPG_PRIVATE_KEY`
- `MAVEN_GPG_PASSPHRASE`

GitHub Packages 使用仓库默认的 `GITHUB_TOKEN`。
