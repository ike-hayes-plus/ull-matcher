# 安全边界

ull-matcher 设计为**内网撮合节点**，不替代统一 API 网关。

## 信任边界

| 组件 | 默认假设 |
| --- | --- |
| HTTP / Binary | 默认 `127.0.0.1`；远程暴露需 ingress API key + 网络隔离 |
| gRPC 复制 | 非 loopback 必须 mTLS（PROD） |
| Aeron | 非 loopback 必须 transport security（PROD） |
| etcd / ZK | 控制面仅在内网；etcd 远程必须 HTTPS + 可选 mTLS |

## PROD 模式闸门

`matcher.serverMode=PROD` 启用：

- WAL 强持久化（禁止 `OS_BUFFERED`）
- 持久化 dataDir：WAL 目录的**任意一级路径段**都不能是 `target`、`build`、`out`，
  这些目录会被构建清理掉
- 非 loopback ingress 必须配置 API keys
- 非 loopback etcd 端点必须 `https://`，该校验在 `EtcdConfig` 构造期生效，
  standalone 和 Spring Boot starter 两条路径都受约束
- 错误响应不返回内部 `detail`

## 入口鉴权

| 入口 | 凭证传递方式 |
| --- | --- |
| HTTP | 请求头 `X-Ull-Api-Key`，或 `Authorization: Bearer <key>` |
| Binary | 连接建立后的**前 32 字节**，UTF-8 密钥右侧补 `0` |

实现要点：

- API key 比较使用**常量时间且不短路**的实现，遍历所有已配置的 key 后再返回，
  不会因为匹配到第一个 key 就提前返回，避免按字节和按 key 顺序的计时侧信道。
- 密钥超过 32 UTF-8 字节会在 `IngressAuthConfig` 构造期被拒绝，不会静默截断成一个更弱的前缀。
- 握手失败时服务端直接关闭连接，不回错误帧，不区分「key 不存在」和「key 错误」。

### 端点鉴权矩阵

| 端点 | 鉴权 | 暴露的信息 |
| --- | --- | --- |
| `GET /api/v1/runtime/live` | 否 | 仅 `{"status":"UP"}` |
| `GET /api/v1/runtime/health` | 是 | 节点角色、WAL 水位、复制状态 |
| `GET /api/v1/runtime/readiness` | 是 | 追赶进度、standby 数量 |
| `GET /metrics` | 是 | Prometheus 指标 |
| 其余业务端点 | 是 | — |

上表「是」表示路由标记了 `requireIngressAuth`。**只有配置了 `matcher.ingressApiKeys` 时鉴权才生效**；未配置时这些路由会放行。PROD 绑定非 loopback 时必须配置密钥，否则进程拒启。

只有 `live` 免鉴权，因为它不泄露任何集群拓扑。LB 和 k8s livenessProbe 用它；
readinessProbe 若要打 `/runtime/readiness`，需要在 probe 上配置 API key 请求头。

## 二进制入口的资源上限

未鉴权的连接是最廉价的攻击面，因此有三道硬上限：

| 系统属性 | 默认值 | 作用 |
| --- | --- | --- |
| `matcher.binaryIngressMaxConnections` | `4096` | 并发连接上限，超限的新连接立即关闭 |
| `matcher.binaryIngressHandshakeTimeoutMillis` | `5000` | 未完成握手的连接存活上限，防止连接占位 |
| `matcher.binaryIngressIdleTimeoutMillis` | `300000` | 已鉴权连接的空闲回收，`0` 表示不回收 |

每条连接的大块缓冲区在**握手成功之后**才分配，所以未鉴权连接的内存占用是常量级的，
无法靠大量半开连接放大内存消耗。`BinaryOrderIngressServer.connectionMetrics()`
暴露拒绝数、握手失败数、握手超时数和空闲回收数，并导出为
`ull_matcher_binary_*` Prometheus 指标，用于告警。

## etcd mTLS

私钥必须是未加密的 PKCS#8（`-----BEGIN PRIVATE KEY-----`），支持 RSA / EC / Ed25519。
PKCS#1、SEC1 和加密私钥会被明确拒绝并给出 `openssl pkcs8 -topk8 -nocrypt` 转换提示，
而不是报一个难以定位的解析错误。

`matcher.etcdTlsCertChain` 和 `matcher.etcdTlsPrivateKey` 必须同时提供或同时省略。
只配一半会在构造期报错，不会静默退化成单向 TLS。

## 脑裂与 fencing

主节点晋升使用单调递增的 fencing token：晋升时取
`max(提议 epoch, 租约存储当前 epoch + 1)`，以**租约存储**为权威，
而不是以被观测到的（可能已过期的）主节点令牌为准。这保证反复切主时 epoch 严格递增，
旧主携带的过期令牌会被后端拒绝。

## 供应链

| 机制 | 触发 |
| --- | --- |
| CodeQL（`security-and-quality`） | 每次 push / PR，每周定时 |
| OWASP dependency-check（CVSS ≥ 7 失败） | 每次 CI |
| CycloneDX SBOM | 每次 CI，作为构建产物上传 |
| Dependabot | 按 `.github/dependabot.yml` 配置 |

## 仍由上游负责

- 账户、风控、清结算
- 公网 TLS 终止与身份联邦
- WAF / 速率限制（节点内 admission 仅做租户预算）
- API key 的签发、轮换与吊销（本项目只做校验，不管理生命周期）
