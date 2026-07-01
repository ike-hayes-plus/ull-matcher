# 部署模式

## 原则

`ull-matcher` 支持两类部署模式：

1. **宿主机/裸机优先模式**
2. **容器化验证与联调模式**

两类模式都有效，但目标不同。

---

## 1. 宿主机 / 裸机优先模式

适用场景：

- 低延迟热点交易对
- 需要更强 CPU 隔离
- 需要 NUMA 感知
- 需要尽量减少容器层调度干扰

建议：

- 独占物理核或独占 NUMA 节点
- 本地 NVMe
- 关闭不必要后台任务
- 外部接入层使用 Envoy / NLB / Ingress

示例：

```bash
taskset -c 2-9 \
numactl --cpunodebind=0 --membind=0 \
java \
  -Dmatcher.serverMode=PROD \
  -Dmatcher.httpBindHost=10.0.0.21 \
  -Dmatcher.allowInsecureRemoteHttp=true \
  -Dmatcher.grpcBindHost=10.0.0.21 \
  -Dmatcher.grpcTlsCertChain=/etc/ull-matcher/tls/server.crt \
  -Dmatcher.grpcTlsPrivateKey=/etc/ull-matcher/tls/server.key \
  -Dmatcher.grpcTlsTrustChain=/etc/ull-matcher/tls/ca.crt \
  -Dmatcher.grpcMtlsRequired=true \
  io.github.ike.ullmatcher.server.bootstrap.MatcherServerMain
```

说明：

- `taskset` / `numactl` 用于提升 CPU 与内存局部性
- gRPC 建议在生产模式下启用 TLS/mTLS
- 项目部署边界是受控内网；HTTP / binary ingress 不设计为直接公网暴露入口
- HTTP 默认绑定 `127.0.0.1`，适合由同机 sidecar、内网网关或内网服务调用
- 内网部署时，HTTP 明文入口应只暴露在受控 VPC / 子网 / 安全组内，不建议直接暴露到公网
- HTTP 是否允许非 loopback 明文监听，必须显式通过 `matcher.allowInsecureRemoteHttp=true` 确认
- Standalone 与 Spring Boot starter 的 WAL 默认值一致：`SYNC_PER_COMMAND`、`walForceBatchSize=1`、`walForceMaxDelayMicros=0`

## 2. 部署配置模型

生产集群使用 `scripts/deploy/cluster.sh` 管理节点启动、停止、滚动重启和健康检查。配置文件分为两层：

- [`scripts/deploy/default.conf`](../../scripts/deploy/default.conf)：基础默认值，包括 HTTP/gRPC/Aeron/binary 默认端口、bind host、WAL、复制模式、failover、TLS、部署健康检查参数。
- [`scripts/deploy/cluster.conf.example`](../../scripts/deploy/cluster.conf.example)：完整集群配置模板，只用于复制，不应直接作为 `cluster.sh -c` 的输入。

创建真实配置：

```bash
mkdir -p conf
cp scripts/deploy/cluster.conf.example conf/merchant-42.env
```

真实配置文件只覆盖当前环境需要改变的值，并提供 `NODES` 清单：

```bash
NODES=(
  "node-a|10.0.1.11|$HTTP_PORT|$GRPC_PORT|$AERON_PORT|$BINARY_PORT|/data/ull-matcher/node-a|/var/log/ull-matcher/node-a"
  "node-b|10.0.1.12|$HTTP_PORT|$GRPC_PORT|$AERON_PORT|$BINARY_PORT|/data/ull-matcher/node-b|/var/log/ull-matcher/node-b"
  "node-c|10.0.1.13|$HTTP_PORT|$GRPC_PORT|$AERON_PORT|$BINARY_PORT|/data/ull-matcher/node-c|/var/log/ull-matcher/node-c"
)
```

端口语义：

- `HTTP_PORT`：HTTP 管理、健康检查、metrics、可选 HTTP 写入口。
- `GRPC_PORT`：`REPLICATION_TRANSPORT=GRPC` 时的主备复制端口。
- `AERON_PORT`：`REPLICATION_TRANSPORT=AERON` 或 `AERON_PREVIEW` 时的复制端口。
- `BINARY_PORT`：binary ingress 写入口；设置为 `-` 可禁用该节点 binary ingress。

执行入口：

```bash
scripts/deploy/cluster.sh -c conf/merchant-42.env plan
scripts/deploy/cluster.sh -c conf/merchant-42.env validate
scripts/deploy/cluster.sh -c conf/merchant-42.env start
scripts/deploy/cluster.sh -c conf/merchant-42.env status
scripts/deploy/cluster.sh -c conf/merchant-42.env restart node-b
scripts/deploy/cluster.sh -c conf/merchant-42.env stop
```

`cluster.sh` 必须显式传入 `-c`，并拒绝直接使用 `*.example`。这样可以避免把示例 IP、目录、端口当作真实生产配置部署。

生产发布前必须先执行 `validate`。如果配置校验、远程目录、端口、数据目录或健康检查任一项失败，不得继续执行 `start` 或 `restart`。

单节点停止会先校验集群健康状态；停止目标 primary 时必须确认当前集群单主且 standby 数量满足 failover 策略。只有紧急停机时才使用：

```bash
scripts/deploy/cluster.sh -c conf/merchant-42.env stop --force node-a
```

---

## 3. Docker Compose 验证模式

适用场景：

- 本地联调
- HA 控制面演练
- ZooKeeper / etcd / chaos 入口验证

仓库提供的是**基础设施 compose**，而不是把 `matcher-server` 变成“容器优先产品”。

文件：

- [`deploy/docker-compose/infra-lab/docker-compose.yml`](../../deploy/docker-compose/infra-lab/docker-compose.yml)
- [`deploy/docker-compose/chaos-lab/docker-compose.yml`](../../deploy/docker-compose/chaos-lab/docker-compose.yml)

用途：

- 拉起 ZooKeeper
- 拉起 etcd
- 拉起 Toxiproxy
- 拉起 Prometheus
- 让 matcher 节点仍然在宿主机或独立进程中运行

启动：

```bash
docker compose -f deploy/docker-compose/infra-lab/docker-compose.yml up -d
```

或：

```bash
./scripts/chaos/lab.sh up
```

---

## 4. 为什么不把 matcher-server 主线做成容器优先

原因：

- 低延迟撮合更看重 CPU/NUMA/磁盘局部性
- 容器编排不一定是最高性能部署方式
- 项目更适合“可容器化”，而不是“必须容器化”

结论：

- **验证环境**：可用 Docker Compose
- **性能优先生产环境**：优先宿主机 / 裸机 / 独占资源部署
