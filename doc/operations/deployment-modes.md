# 部署模式建议

## 原则

`ull-matcher` 支持两类部署模式：

1. **宿主机/裸机优先模式**
2. **容器化实验与联调模式**

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
- HTTP 是否允许远端明文暴露，必须显式通过 `matcher.allowInsecureRemoteHttp=true` 确认

---

## 2. Docker Compose 实验模式

适用场景：

- 本地联调
- HA 控制面演练
- ZooKeeper / Nacos / chaos 入口验证

当前仓库提供的是**基础设施 compose**，而不是把 `matcher-server` 变成“容器优先产品”。

文件：

- [`deploy/docker-compose/infra-lab/docker-compose.yml`](../../deploy/docker-compose/infra-lab/docker-compose.yml)
- [`deploy/docker-compose/chaos-lab/docker-compose.yml`](../../deploy/docker-compose/chaos-lab/docker-compose.yml)

用途：

- 拉起 ZooKeeper
- 拉起 Nacos
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

## 3. 为什么不把 matcher-server 主线做成容器优先

原因：

- 低延迟撮合更看重 CPU/NUMA/磁盘局部性
- 容器编排不一定是最高性能部署方式
- 当前项目更适合“可容器化”，而不是“必须容器化”

结论：

- **实验环境**：可用 Docker Compose
- **性能优先生产环境**：优先宿主机 / 裸机 / 独占资源部署
