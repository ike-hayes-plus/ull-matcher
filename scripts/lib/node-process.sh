#!/usr/bin/env bash

start_matcher_node() {
  if [[ "${#}" -lt 4 || "${#}" -gt 5 ]]; then
    echo "usage: $0 <node-id> <http-port> <grpc-port> <aeron-port> [binary-port]" >&2
    return 1
  fi

  local node_id="$1"
  local http_port="$2"
  local grpc_port="$3"
  local aeron_port="$4"
  local binary_port="${5:-}"

  local root_dir="${ROOT_DIR:?ROOT_DIR is required}"
  local java_bin="${JAVA_BIN:?JAVA_BIN is required}"
  local mode_label="${MODE_LABEL:-node}"
  local server_mode="${SERVER_MODE_VALUE:-DEV}"
  local shard_key="${SHARD_KEY:-merchant:42}"
  local symbol_id="${SYMBOL_ID:-1}"
  local cluster_name="${CLUSTER_NAME:-merchant-cluster}"
  local zk_connect="${ZK_CONNECT:-127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183}"
  local etcd_endpoint="${ETCD_ENDPOINT:-http://127.0.0.1:2379,http://127.0.0.1:2381,http://127.0.0.1:2383}"
  local lease_provider="${LEASE_PROVIDER:-${MATCHER_LEASE_PROVIDER:-zk}}"
  local discovery_provider="${DISCOVERY_PROVIDER:-${MATCHER_DISCOVERY_PROVIDER:-zk}}"
  local data_root="${DATA_ROOT:?DATA_ROOT is required}"
  local log_root="${LOG_ROOT:?LOG_ROOT is required}"
  local matcher_data_dir="${MATCHER_DATA_DIR:-}"
  local cp_file="${CP_FILE:?CP_FILE is required}"
  local class_name="io.github.ike.ullmatcher.server.bootstrap.MatcherServerMain"
  local advertised_host="${ADVERTISED_HOST:-127.0.0.1}"
  local http_bind_host="${HTTP_BIND_HOST:-0.0.0.0}"
  local grpc_bind_host="${GRPC_BIND_HOST:-0.0.0.0}"
  local binary_bind_host="${BINARY_BIND_HOST:-0.0.0.0}"
  local replication_transport="${REPLICATION_TRANSPORT:-GRPC}"
  local transport_change_window_id="${TRANSPORT_CHANGE_WINDOW_ID:-}"
  local allow_transport_change="${ALLOW_TRANSPORT_CHANGE:-false}"
  local enable_transport_tls="${ENABLE_TRANSPORT_TLS:-false}"
  local transport_tls_dir="${TRANSPORT_TLS_DIR:-$data_root/tls}"
  local transport_tls_reload_millis="${TRANSPORT_TLS_RELOAD_MILLIS:-250}"
  local wal_durability_mode="${WAL_DURABILITY_MODE:-}"
  local wal_force_batch_size="${WAL_FORCE_BATCH_SIZE:-}"
  local wal_force_max_delay_micros="${WAL_FORCE_MAX_DELAY_MICROS:-}"
  local replication_mode="${REPLICATION_MODE:-${MATCHER_REPLICATION_MODE:-}}"
  local failover_min_standby_replicas="${FAILOVER_MIN_STANDBY_REPLICAS:-${MATCHER_FAILOVER_MIN_STANDBY_REPLICAS:-}}"
  local failover_primary_heartbeat_timeout_millis="${FAILOVER_PRIMARY_HEARTBEAT_TIMEOUT_MILLIS:-}"
  local failover_max_promotion_lag="${FAILOVER_MAX_PROMOTION_LAG:-}"
  local cp_build_maven_args="${CP_BUILD_MAVEN_ARGS:- -q -pl matcher-server -am -DskipTests package dependency:build-classpath}"
  local java_opts=()

  assert_port_free() {
    local port="$1"
    if lsof -tiTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "[$mode_label] port $port is already in use; stop the existing process before starting $node_id" >&2
      return 3
    fi
  }

  if [[ "$replication_transport" == "AERON_PREVIEW" || "$replication_transport" == "AERON" ]]; then
    java_opts+=(--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED)
    java_opts+=(--add-opens=java.base/sun.nio.ch=ALL-UNNAMED)
  fi

  mkdir -p "$data_root" "$log_root"

  assert_port_free "$http_port"
  assert_port_free "$grpc_port"
  if [[ -n "$binary_port" ]]; then
    assert_port_free "$binary_port"
  fi

  if [[ ! -f "$cp_file" ]]; then
    echo "[$mode_label] building matcher-server runtime classpath"
    (
      cd "$root_dir"
      export JAVA_HOME="$JAVA_HOME" PATH="$JAVA_HOME/bin:$PATH"
      # shellcheck disable=SC2086
      mvn $cp_build_maven_args -Dmdep.outputFile="$cp_file" -Dmdep.pathSeparator=:
    )
  fi

  local module_cp="$root_dir/matcher-server/target/classes:$root_dir/matcher-core/target/classes:$root_dir/matcher-storage/target/classes:$root_dir/matcher-runtime/target/classes:$root_dir/matcher-ha/target/classes:$root_dir/matcher-ha-grpc/target/classes:$root_dir/matcher-ha-aeron/target/classes:$root_dir/matcher-ha-zookeeper/target/classes:$root_dir/matcher-ha-etcd/target/classes:$root_dir/matcher-discovery-zookeeper/target/classes:$root_dir/matcher-spring-boot-starter/target/classes:$root_dir/matcher-examples/target/classes"
  local dependency_cp
  dependency_cp="$(cat "$cp_file")"
  local full_cp="$module_cp:$dependency_cp"

  local data_dir="${matcher_data_dir:-$data_root/$node_id}"
  local log_file="$log_root/$node_id.log"
  local pid_file="$log_root/$node_id.pid"
  local ports_file="$log_root/$node_id.ports"

  mkdir -p "$data_dir"
  rm -f "$pid_file"
  rm -f "$ports_file"

  local cmd=(
    "$java_bin"
    ${java_opts[@]+"${java_opts[@]}"}
    -Dmatcher.serverMode="$server_mode"
    -Dmatcher.nodeId="$node_id"
    -Dmatcher.symbolId="$symbol_id"
    -Dmatcher.shardKey="$shard_key"
    -Dmatcher.dataDir="$data_dir"
    -Dmatcher.httpPort="$http_port"
    -Dmatcher.httpBindHost="$http_bind_host"
    -Dmatcher.grpcPort="$grpc_port"
    -Dmatcher.grpcBindHost="$grpc_bind_host"
    -Dmatcher.zkConnect="$zk_connect"
    -Dmatcher.etcdEndpoint="$etcd_endpoint"
    -Dmatcher.leaseProvider="$lease_provider"
    -Dmatcher.discoveryProvider="$discovery_provider"
    -Dmatcher.cluster="$cluster_name"
    -Dmatcher.advertisedHost="$advertised_host"
    -Dmatcher.replicationTransport="$replication_transport"
    -Dmatcher.aeronPreviewDirectory="$data_dir/aeron-preview"
    -Dmatcher.aeronPreviewPort="$aeron_port"
  )

  if [[ -n "$binary_port" ]]; then
    cmd+=(
      -Dmatcher.binaryIngressEnabled=true
      -Dmatcher.binaryIngressPort="$binary_port"
      -Dmatcher.binaryIngressBindHost="$binary_bind_host"
    )
  fi

  if [[ "$allow_transport_change" == "true" ]]; then
    cmd+=(-Dmatcher.allowTransportChange=true)
  fi
  if [[ -n "$transport_change_window_id" ]]; then
    cmd+=(-Dmatcher.transportChangeWindowId="$transport_change_window_id")
  fi
  if [[ "$enable_transport_tls" == "true" ]]; then
    local node_tls_dir="$transport_tls_dir/$node_id"
    if [[ ! -f "$node_tls_dir/tls.crt" || ! -f "$node_tls_dir/tls.key" || ! -f "$node_tls_dir/ca.crt" ]]; then
      echo "[$mode_label] missing TLS files for $node_id under $node_tls_dir" >&2
      return 2
    fi
    cmd+=(
      -Dmatcher.transportTlsCertChain="$node_tls_dir/tls.crt"
      -Dmatcher.transportTlsPrivateKey="$node_tls_dir/tls.key"
      -Dmatcher.transportTlsTrustChain="$node_tls_dir/ca.crt"
      -Dmatcher.transportMtlsRequired=true
      -Dmatcher.transportTlsReloadMillis="$transport_tls_reload_millis"
    )
  fi
  if [[ -n "$wal_durability_mode" ]]; then
    cmd+=(-Dmatcher.walDurabilityMode="$wal_durability_mode")
  fi
  if [[ -n "$wal_force_batch_size" ]]; then
    cmd+=(-Dmatcher.walForceBatchSize="$wal_force_batch_size")
  fi
  if [[ -n "$wal_force_max_delay_micros" ]]; then
    cmd+=(-Dmatcher.walForceMaxDelayMicros="$wal_force_max_delay_micros")
  fi
  if [[ -n "$replication_mode" ]]; then
    cmd+=(-Dmatcher.replicationMode="$replication_mode")
  fi
  if [[ -n "$failover_min_standby_replicas" ]]; then
    cmd+=(-Dmatcher.failoverMinStandbyReplicas="$failover_min_standby_replicas")
  fi
  if [[ -n "$failover_primary_heartbeat_timeout_millis" ]]; then
    cmd+=(-Dmatcher.failoverPrimaryHeartbeatTimeoutMillis="$failover_primary_heartbeat_timeout_millis")
  fi
  if [[ -n "$failover_max_promotion_lag" ]]; then
    cmd+=(-Dmatcher.failoverMaxPromotionLag="$failover_max_promotion_lag")
  fi

  cmd+=(-cp "$full_cp" "$class_name")

  nohup "${cmd[@]}" </dev/null >"$log_file" 2>&1 &
  echo $! > "$pid_file"
  {
    echo "http=$http_port"
    echo "grpc=$grpc_port"
    echo "aeron=$aeron_port"
    if [[ -n "$binary_port" ]]; then
      echo "binary=$binary_port"
    fi
  } > "$ports_file"
  echo "[$mode_label] started $node_id http=$http_port grpc=$grpc_port binary=${binary_port:--} transport=$replication_transport pid=$(cat "$pid_file") log=$log_file"
}

stop_matcher_node() {
  if [[ "${#}" -ne 1 ]]; then
    echo "usage: $0 <node-id>" >&2
    return 1
  fi

  local node_id="$1"
  local root_dir="${ROOT_DIR:?ROOT_DIR is required}"
  local log_root="${LOG_ROOT:?LOG_ROOT is required}"
  local mode_label="${MODE_LABEL:-node}"
  local pid_file="$log_root/$node_id.pid"
  local ports_file="$log_root/$node_id.ports"

  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" || true
      wait "$pid" 2>/dev/null || true
    fi
  fi

  local ports=()
  if [[ -f "$ports_file" ]]; then
    while IFS='=' read -r _name value; do
      [[ -n "$value" ]] || continue
      ports+=("$value")
    done < "$ports_file"
  elif [[ -n "${PORT_RESOLVER:-}" ]]; then
    local port_resolver="${PORT_RESOLVER}"
    while read -r port; do
      [[ -n "$port" ]] || continue
      ports+=("$port")
    done < <($port_resolver "$node_id" | tr ' ' '\n')
  fi

  for port in "${ports[@]}"; do
    while read -r pid; do
      [[ -n "$pid" ]] || continue
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    done < <(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
  done

  rm -f "$pid_file"
  rm -f "$ports_file"
  echo "[$mode_label] stopped $node_id"
}
