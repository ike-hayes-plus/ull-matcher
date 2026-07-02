package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.ha.grpc.telemetry.GrpcTransportMetrics;
import io.github.ike.ullmatcher.ha.state.NodeControlState;
import io.github.ike.ullmatcher.server.cluster.ClusterSupervisorMetricsSnapshot;
import io.github.ike.ullmatcher.server.telemetry.MatcherNodeMetricsSnapshot;
import io.github.ike.ullmatcher.server.telemetry.ReadinessSnapshot;

import java.util.List;
import java.util.Map;

final class PrometheusMetricsExporter {
    private PrometheusMetricsExporter() {}

    static String export(NodeControlState state,
                         MatcherNodeMetricsSnapshot nodeMetrics,
                         GrpcTransportMetrics.Snapshot grpc,
                         ClusterSupervisorMetricsSnapshot cluster,
                         ReadinessSnapshot readiness,
                         HttpMetrics http) {
        StringBuilder builder = new StringBuilder(HttpApiServer.METRICS_BUFFER_INITIAL_CAPACITY)
                .append("# TYPE ull_matcher_grpc_unary_replications_total counter\n")
                .append("ull_matcher_grpc_unary_replications_total ").append(grpc.unaryReplications()).append('\n')
                .append("# TYPE ull_matcher_grpc_stream_batches_total counter\n")
                .append("ull_matcher_grpc_stream_batches_total ").append(grpc.streamedBatches()).append('\n')
                .append("# TYPE ull_matcher_grpc_stream_commands_total counter\n")
                .append("ull_matcher_grpc_stream_commands_total ").append(grpc.streamedCommands()).append('\n')
                .append("# TYPE ull_matcher_grpc_snapshot_bytes_sent_total counter\n")
                .append("ull_matcher_grpc_snapshot_bytes_sent_total ").append(grpc.snapshotBytesSent()).append('\n')
                .append("# TYPE ull_matcher_grpc_snapshot_bytes_received_total counter\n")
                .append("ull_matcher_grpc_snapshot_bytes_received_total ").append(grpc.snapshotBytesReceived()).append('\n')
                .append("# TYPE ull_matcher_grpc_rejected_ingress_total counter\n")
                .append("ull_matcher_grpc_rejected_ingress_total ").append(grpc.rejectedIngress()).append('\n')
                .append("# TYPE ull_matcher_grpc_failures_total counter\n")
                .append("ull_matcher_grpc_failures_total ").append(grpc.failures()).append('\n')
                .append("# TYPE ull_matcher_role gauge\n")
                .append("ull_matcher_role{role=\"").append(state.role().name()).append("\"} 1\n")
                .append("# TYPE ull_matcher_last_applied_sequence gauge\n")
                .append("ull_matcher_last_applied_sequence ").append(state.cursor().lastAppliedSequence()).append('\n')
                .append("# TYPE ull_matcher_snapshot_sequence gauge\n")
                .append("ull_matcher_snapshot_sequence ").append(state.cursor().snapshotSequence()).append('\n')
                .append("# TYPE ull_matcher_live_orders gauge\n")
                .append("ull_matcher_live_orders ").append(nodeMetrics.liveOrderCount()).append('\n')
                .append("# TYPE ull_matcher_last_trade_id gauge\n")
                .append("ull_matcher_last_trade_id ").append(nodeMetrics.lastTradeId()).append('\n')
                .append("# TYPE ull_matcher_loop_processed_commands counter\n")
                .append("ull_matcher_loop_processed_commands ").append(nodeMetrics.loopSnapshot().processedCommandCount()).append('\n')
                .append("# TYPE ull_matcher_trade_events_total counter\n")
                .append("ull_matcher_trade_events_total ").append(nodeMetrics.matchingMetrics().tradeCount()).append('\n')
                .append("# TYPE ull_matcher_order_events_total counter\n")
                .append("ull_matcher_order_events_total ").append(nodeMetrics.matchingMetrics().orderEventCount()).append('\n')
                .append("# TYPE ull_matcher_rejected_commands_total counter\n")
                .append("ull_matcher_rejected_commands_total ").append(nodeMetrics.matchingMetrics().rejectedCommandCount()).append('\n')
                .append("# TYPE ull_matcher_capacity_rejected_commands_total counter\n")
                .append("ull_matcher_capacity_rejected_commands_total ").append(nodeMetrics.matchingMetrics().capacityRejectedCommandCount()).append('\n')
                .append("# TYPE ull_matcher_wal_segments gauge\n")
                .append("ull_matcher_wal_segments ").append(nodeMetrics.walSegmentCount()).append('\n')
                .append("# TYPE ull_matcher_wal_current_segment_bytes gauge\n")
                .append("ull_matcher_wal_current_segment_bytes ").append(nodeMetrics.currentWalSegmentBytes()).append('\n')
                .append("# TYPE ull_matcher_submit_queue_depth gauge\n")
                .append("ull_matcher_submit_queue_depth ").append(nodeMetrics.submitPathMetrics().submitQueueDepth()).append('\n')
                .append("# TYPE ull_matcher_submit_queue_capacity gauge\n")
                .append("ull_matcher_submit_queue_capacity ").append(nodeMetrics.submitPathMetrics().submitQueueCapacity()).append('\n')
                .append("# TYPE ull_matcher_ring_depth gauge\n")
                .append("ull_matcher_ring_depth ").append(nodeMetrics.submitPathMetrics().ringDepth()).append('\n')
                .append("# TYPE ull_matcher_ring_remaining_capacity gauge\n")
                .append("ull_matcher_ring_remaining_capacity ").append(nodeMetrics.submitPathMetrics().ringRemainingCapacity()).append('\n')
                .append("# TYPE ull_matcher_gateway_accepted_total counter\n")
                .append("ull_matcher_gateway_accepted_total ").append(nodeMetrics.submitPathMetrics().walAcceptedTotal()).append('\n')
                .append("# TYPE ull_matcher_gateway_wal_appended_total counter\n")
                .append("ull_matcher_gateway_wal_appended_total ").append(nodeMetrics.submitPathMetrics().walAppendedTotal()).append('\n')
                .append("# TYPE ull_matcher_gateway_wal_forced_total counter\n")
                .append("ull_matcher_gateway_wal_forced_total ").append(nodeMetrics.submitPathMetrics().walForcedTotal()).append('\n')
                .append("# TYPE ull_matcher_gateway_failed_before_wal_total counter\n")
                .append("ull_matcher_gateway_failed_before_wal_total ").append(nodeMetrics.submitPathMetrics().failedBeforeWalTotal()).append('\n')
                .append("# TYPE ull_matcher_gateway_failed_after_wal_total counter\n")
                .append("ull_matcher_gateway_failed_after_wal_total ").append(nodeMetrics.submitPathMetrics().failedAfterWalTotal()).append('\n')
                .append("# TYPE ull_matcher_submission_tracked gauge\n")
                .append("ull_matcher_submission_tracked ").append(nodeMetrics.submissionMetrics().trackedCount()).append('\n')
                .append("# TYPE ull_matcher_submission_pending gauge\n")
                .append("ull_matcher_submission_pending ").append(nodeMetrics.submissionMetrics().pendingCount()).append('\n')
                .append("# TYPE ull_matcher_submission_committed gauge\n")
                .append("ull_matcher_submission_committed ").append(nodeMetrics.submissionMetrics().committedCount()).append('\n')
                .append("# TYPE ull_matcher_submission_failed gauge\n")
                .append("ull_matcher_submission_failed ").append(nodeMetrics.submissionMetrics().failedCount()).append('\n')
                .append("# TYPE ull_matcher_submission_retrying gauge\n")
                .append("ull_matcher_submission_retrying ").append(nodeMetrics.submissionMetrics().retryingCount()).append('\n')
                .append("# TYPE ull_matcher_submission_committed_total counter\n")
                .append("ull_matcher_submission_committed_total ").append(nodeMetrics.submissionMetrics().committedTotal()).append('\n')
                .append("# TYPE ull_matcher_submission_failed_total counter\n")
                .append("ull_matcher_submission_failed_total ").append(nodeMetrics.submissionMetrics().failedTotal()).append('\n')
                .append("# TYPE ull_matcher_replication_queue_depth gauge\n")
                .append("ull_matcher_replication_queue_depth ").append(nodeMetrics.replicationMetrics().queueDepth()).append('\n')
                .append("# TYPE ull_matcher_replication_queue_capacity gauge\n")
                .append("ull_matcher_replication_queue_capacity ").append(nodeMetrics.replicationMetrics().queueCapacity()).append('\n')
                .append("# TYPE ull_matcher_replication_queue_high_watermark gauge\n")
                .append("ull_matcher_replication_queue_high_watermark ").append(nodeMetrics.replicationMetrics().maxObservedQueueDepth()).append('\n')
                .append("# TYPE ull_matcher_replication_last_batch_size gauge\n")
                .append("ull_matcher_replication_last_batch_size ").append(nodeMetrics.replicationMetrics().lastBatchSize()).append('\n')
                .append("# TYPE ull_matcher_replication_batch_high_watermark gauge\n")
                .append("ull_matcher_replication_batch_high_watermark ").append(nodeMetrics.replicationMetrics().maxObservedBatchSize()).append('\n')
                .append("# TYPE ull_matcher_replication_batches_total counter\n")
                .append("ull_matcher_replication_batches_total ").append(nodeMetrics.replicationMetrics().batchesReplicatedTotal()).append('\n')
                .append("# TYPE ull_matcher_replication_commands_total counter\n")
                .append("ull_matcher_replication_commands_total ").append(nodeMetrics.replicationMetrics().commandsReplicatedTotal()).append('\n')
                .append("# TYPE ull_matcher_replication_committed_sequence gauge\n")
                .append("ull_matcher_replication_committed_sequence ").append(nodeMetrics.replicationMetrics().lastCommittedSequence()).append('\n')
                .append("# TYPE ull_matcher_replication_retries_total counter\n")
                .append("ull_matcher_replication_retries_total ").append(nodeMetrics.replicationMetrics().retryCount()).append('\n')
                .append("# TYPE ull_matcher_replication_last_accumulation_micros gauge\n")
                .append("ull_matcher_replication_last_accumulation_micros ").append(nodeMetrics.replicationMetrics().lastAccumulationMicros()).append('\n')
                .append("# TYPE ull_matcher_replication_last_commit_micros gauge\n")
                .append("ull_matcher_replication_last_commit_micros ").append(nodeMetrics.replicationMetrics().lastCommitMicros()).append('\n')
                .append("# TYPE ull_matcher_replication_last_backoff_micros gauge\n")
                .append("ull_matcher_replication_last_backoff_micros ").append(nodeMetrics.replicationMetrics().lastBackoffMicros()).append('\n')
                .append("# TYPE ull_matcher_standby_apply_queue_depth gauge\n")
                .append("ull_matcher_standby_apply_queue_depth ").append(nodeMetrics.standbySyncMetrics().applyQueueDepth()).append('\n')
                .append("# TYPE ull_matcher_standby_apply_queue_capacity gauge\n")
                .append("ull_matcher_standby_apply_queue_capacity ").append(nodeMetrics.standbySyncMetrics().applyQueueCapacity()).append('\n')
                .append("# TYPE ull_matcher_standby_apply_queue_high_watermark gauge\n")
                .append("ull_matcher_standby_apply_queue_high_watermark ").append(nodeMetrics.standbySyncMetrics().maxObservedApplyQueueDepth()).append('\n')
                .append("# TYPE ull_matcher_standby_last_replicated_batch_size gauge\n")
                .append("ull_matcher_standby_last_replicated_batch_size ").append(nodeMetrics.standbySyncMetrics().lastReplicatedBatchSize()).append('\n')
                .append("# TYPE ull_matcher_standby_replicated_batch_high_watermark gauge\n")
                .append("ull_matcher_standby_replicated_batch_high_watermark ").append(nodeMetrics.standbySyncMetrics().maxObservedReplicatedBatchSize()).append('\n')
                .append("# TYPE ull_matcher_standby_replicated_batches_total counter\n")
                .append("ull_matcher_standby_replicated_batches_total ").append(nodeMetrics.standbySyncMetrics().replicatedBatchesTotal()).append('\n')
                .append("# TYPE ull_matcher_standby_replicated_commands_total counter\n")
                .append("ull_matcher_standby_replicated_commands_total ").append(nodeMetrics.standbySyncMetrics().replicatedCommandsTotal()).append('\n')
                .append("# TYPE ull_matcher_standby_ack_flush_total counter\n")
                .append("ull_matcher_standby_ack_flush_total ").append(nodeMetrics.standbySyncMetrics().ackFlushCount()).append('\n')
                .append("# TYPE ull_matcher_standby_last_ack_flush_commands gauge\n")
                .append("ull_matcher_standby_last_ack_flush_commands ").append(nodeMetrics.standbySyncMetrics().lastAckFlushCommands()).append('\n')
                .append("# TYPE ull_matcher_standby_last_ack_flush_micros gauge\n")
                .append("ull_matcher_standby_last_ack_flush_micros ").append(nodeMetrics.standbySyncMetrics().lastAckFlushMicros()).append('\n')
                .append("# TYPE ull_matcher_standby_last_ack_flush_interval_micros gauge\n")
                .append("ull_matcher_standby_last_ack_flush_interval_micros ").append(nodeMetrics.standbySyncMetrics().lastAckFlushIntervalMicros()).append('\n')
                .append("# TYPE ull_matcher_readiness_service_ready gauge\n")
                .append("ull_matcher_readiness_service_ready ").append(readiness.serviceReady() ? 1 : 0).append('\n')
                .append("# TYPE ull_matcher_readiness_client_traffic_ready gauge\n")
                .append("ull_matcher_readiness_client_traffic_ready ").append(readiness.clientTrafficReady() ? 1 : 0).append('\n')
                .append("# TYPE ull_matcher_transport_security_generation gauge\n")
                .append("ull_matcher_transport_security_generation ").append(readiness.transportSecurityGeneration()).append('\n')
                .append("# TYPE ull_matcher_transport_security_reload_count_total counter\n")
                .append("ull_matcher_transport_security_reload_count_total ").append(readiness.transportSecurityReloadCount()).append('\n')
                .append("# TYPE ull_matcher_transport_security_reload_failures_total counter\n")
                .append("ull_matcher_transport_security_reload_failures_total ").append(readiness.transportSecurityFailureCount()).append('\n')
                .append("# TYPE ull_matcher_transport_security_reloading gauge\n")
                .append("ull_matcher_transport_security_reloading ").append(readiness.tlsReloadInProgress() ? 1 : 0).append('\n');

        appendClusterMetrics(builder, cluster);
        appendHttpMetrics(builder, http);

        return builder.append("# TYPE ull_matcher_ttl_active_tracked_orders gauge\n")
                .append("ull_matcher_ttl_active_tracked_orders ").append(nodeMetrics.ttlMetrics().activeTrackedOrders()).append('\n')
                .append("# TYPE ull_matcher_ttl_pending_submissions gauge\n")
                .append("ull_matcher_ttl_pending_submissions ").append(nodeMetrics.ttlMetrics().pendingSubmissions()).append('\n')
                .append("# TYPE ull_matcher_ttl_scheduled_total counter\n")
                .append("ull_matcher_ttl_scheduled_total ").append(nodeMetrics.ttlMetrics().scheduledTotal()).append('\n')
                .append("# TYPE ull_matcher_ttl_cancel_requested_total counter\n")
                .append("ull_matcher_ttl_cancel_requested_total ").append(nodeMetrics.ttlMetrics().cancelRequestedTotal()).append('\n')
                .append("# TYPE ull_matcher_ttl_cancel_accepted_total counter\n")
                .append("ull_matcher_ttl_cancel_accepted_total ").append(nodeMetrics.ttlMetrics().cancelAcceptedTotal()).append('\n')
                .append("# TYPE ull_matcher_ttl_cancel_skipped_total counter\n")
                .append("ull_matcher_ttl_cancel_skipped_total ").append(nodeMetrics.ttlMetrics().cancelSkippedTotal()).append('\n')
                .append("# TYPE ull_matcher_ttl_cancel_failed_total counter\n")
                .append("ull_matcher_ttl_cancel_failed_total ").append(nodeMetrics.ttlMetrics().cancelFailedTotal()).append('\n')
                .toString();
    }

    private static void appendClusterMetrics(StringBuilder builder, ClusterSupervisorMetricsSnapshot cluster) {
        if (cluster.lastGateDecision() != null) {
            builder.append("# TYPE ull_matcher_ha_promotion_ready gauge\n")
                    .append("ull_matcher_ha_promotion_ready ").append(cluster.lastGateDecision().promotionReady() ? 1 : 0).append('\n')
                    .append("# TYPE ull_matcher_ha_snapshot_sync_required gauge\n")
                    .append("ull_matcher_ha_snapshot_sync_required ").append(cluster.lastGateDecision().snapshotSyncRequired() ? 1 : 0).append('\n')
                    .append("# TYPE ull_matcher_ha_received_lag gauge\n")
                    .append("ull_matcher_ha_received_lag ").append(cluster.lastGateDecision().report().receivedLag()).append('\n')
                    .append("# TYPE ull_matcher_ha_durable_lag gauge\n")
                    .append("ull_matcher_ha_durable_lag ").append(cluster.lastGateDecision().report().durableLag()).append('\n')
                    .append("# TYPE ull_matcher_ha_applied_lag gauge\n")
                    .append("ull_matcher_ha_applied_lag ").append(cluster.lastGateDecision().report().appliedLag()).append('\n')
                    .append("# TYPE ull_matcher_ha_snapshot_lag gauge\n")
                    .append("ull_matcher_ha_snapshot_lag ").append(cluster.lastGateDecision().report().snapshotLag()).append('\n');
        }
        builder.append("# TYPE ull_matcher_ha_tick_total counter\n")
                .append("ull_matcher_ha_tick_total ").append(cluster.tickCount()).append('\n')
                .append("# TYPE ull_matcher_ha_tick_failures_total counter\n")
                .append("ull_matcher_ha_tick_failures_total ").append(cluster.tickFailureCount()).append('\n')
                .append("# TYPE ull_matcher_ha_replication_transport gauge\n")
                .append("ull_matcher_ha_replication_transport{transport=\"")
                .append(escapeLabelValue(cluster.transportMetrics().transportType()))
                .append("\"} 1\n")
                .append("# TYPE ull_matcher_ha_transport_preview_published_total counter\n")
                .append("ull_matcher_ha_transport_preview_published_total ")
                .append(cluster.transportMetrics().previewPublishedCommands()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_preview_published_bytes_total counter\n")
                .append("ull_matcher_ha_transport_preview_published_bytes_total ")
                .append(cluster.transportMetrics().previewPublishedBytes()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_preview_publish_failures_total counter\n")
                .append("ull_matcher_ha_transport_preview_publish_failures_total ")
                .append(cluster.transportMetrics().previewPublishFailures()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_preview_received_total counter\n")
                .append("ull_matcher_ha_transport_preview_received_total ")
                .append(cluster.transportMetrics().previewReceivedCommands()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_preview_received_bytes_total counter\n")
                .append("ull_matcher_ha_transport_preview_received_bytes_total ")
                .append(cluster.transportMetrics().previewReceivedBytes()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_snapshot_requests_total counter\n")
                .append("ull_matcher_ha_transport_snapshot_requests_total ")
                .append(cluster.transportMetrics().snapshotRequests()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_snapshot_request_failures_total counter\n")
                .append("ull_matcher_ha_transport_snapshot_request_failures_total ")
                .append(cluster.transportMetrics().snapshotRequestFailures()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_snapshot_bytes_sent_total counter\n")
                .append("ull_matcher_ha_transport_snapshot_bytes_sent_total ")
                .append(cluster.transportMetrics().snapshotBytesSent()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_snapshot_bytes_received_total counter\n")
                .append("ull_matcher_ha_transport_snapshot_bytes_received_total ")
                .append(cluster.transportMetrics().snapshotBytesReceived()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_control_requests_total counter\n")
                .append("ull_matcher_ha_transport_control_requests_total ")
                .append(cluster.transportMetrics().controlRequests()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_control_request_failures_total counter\n")
                .append("ull_matcher_ha_transport_control_request_failures_total ")
                .append(cluster.transportMetrics().controlRequestFailures()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_authoritative_last_received_sequence gauge\n")
                .append("ull_matcher_ha_transport_authoritative_last_received_sequence ")
                .append(cluster.transportMetrics().authoritativeLastReceivedSequence()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_preview_last_received_sequence gauge\n")
                .append("ull_matcher_ha_transport_preview_last_received_sequence ")
                .append(cluster.transportMetrics().previewLastReceivedSequence()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_preview_gap_total counter\n")
                .append("ull_matcher_ha_transport_preview_gap_total ")
                .append(cluster.transportMetrics().previewGapCount()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_preview_out_of_order_total counter\n")
                .append("ull_matcher_ha_transport_preview_out_of_order_total ")
                .append(cluster.transportMetrics().previewOutOfOrderCount()).append('\n')
                .append("# TYPE ull_matcher_ha_transport_reconciliation_status gauge\n")
                .append("ull_matcher_ha_transport_reconciliation_status{status=\"")
                .append(escapeLabelValue(cluster.transportMetrics().reconciliationStatus()))
                .append("\"} 1\n")
                .append("# TYPE ull_matcher_ha_transport_policy_status gauge\n")
                .append("ull_matcher_ha_transport_policy_status{status=\"")
                .append(escapeLabelValue(cluster.transportMetrics().policyStatus()))
                .append("\"} 1\n");
        cluster.errorCounts().forEach((category, count) -> builder
                .append("ull_matcher_ha_error_total{category=\"").append(category).append("\"} ").append(count).append('\n'));
    }

    private static void appendHttpMetrics(StringBuilder builder, HttpMetrics http) {
        int globalInflight = http.maxConcurrentRequests() - http.requestAvailablePermits();
        double globalSaturation = ((double) globalInflight) / http.maxConcurrentRequests();
        builder.append("# TYPE ull_matcher_http_global_overload_total counter\n")
                .append("ull_matcher_http_global_overload_total ").append(http.globalOverloadCount()).append('\n')
                .append("# TYPE ull_matcher_http_global_inflight gauge\n")
                .append("ull_matcher_http_global_inflight ").append(globalInflight).append('\n')
                .append("# TYPE ull_matcher_http_global_saturation gauge\n")
                .append("ull_matcher_http_global_saturation ").append(globalSaturation).append('\n')
                .append("# TYPE ull_matcher_http_executor_queue_depth gauge\n")
                .append("ull_matcher_http_executor_queue_depth ").append(http.executorQueueDepth()).append('\n')
                .append("# TYPE ull_matcher_http_executor_queue_capacity gauge\n")
                .append("ull_matcher_http_executor_queue_capacity ").append(http.executorQueueCapacity()).append('\n');

        WriteAdmissionController writeAdmissionController = http.writeAdmissionController();
        builder.append("# TYPE ull_matcher_http_shard_write_overload_total counter\n")
                .append("ull_matcher_http_shard_write_overload_total{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                .append(writeAdmissionController.shardOverloadCount()).append('\n')
                .append("# TYPE ull_matcher_http_shard_write_inflight gauge\n")
                .append("ull_matcher_http_shard_write_inflight{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                .append(writeAdmissionController.shardInflight()).append('\n')
                .append("# TYPE ull_matcher_http_shard_write_saturation gauge\n")
                .append("ull_matcher_http_shard_write_saturation{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                .append(writeAdmissionController.shardSaturation()).append('\n')
                .append("# TYPE ull_matcher_http_shard_write_rate_limited_total counter\n")
                .append("ull_matcher_http_shard_write_rate_limited_total{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                .append(writeAdmissionController.shardRateLimitedCount()).append('\n')
                .append("# TYPE ull_matcher_http_shard_write_rate_limit_per_second gauge\n")
                .append("ull_matcher_http_shard_write_rate_limit_per_second{shard=\"").append(escapeLabelValue(writeAdmissionController.shardKey())).append("\"} ")
                .append(writeAdmissionController.shardRateLimitPerSecond()).append('\n')
                .append("# TYPE ull_matcher_http_tenant_write_overload_total counter\n")
                .append("ull_matcher_http_tenant_write_overload_total ").append(writeAdmissionController.tenantOverloadCount()).append('\n')
                .append("# TYPE ull_matcher_http_tenant_write_rate_limited_total counter\n")
                .append("ull_matcher_http_tenant_write_rate_limited_total ").append(writeAdmissionController.tenantRateLimitedCount()).append('\n')
                .append("# TYPE ull_matcher_http_tenant_write_rate_limit_per_second gauge\n")
                .append("ull_matcher_http_tenant_write_rate_limit_per_second ").append(writeAdmissionController.tenantRateLimitPerSecond()).append('\n')
                .append("# TYPE ull_matcher_http_tenant_write_default_weight gauge\n")
                .append("ull_matcher_http_tenant_write_default_weight ").append(writeAdmissionController.tenantDefaultWeight()).append('\n')
                .append("# TYPE ull_matcher_http_tenant_write_active_entries gauge\n")
                .append("ull_matcher_http_tenant_write_active_entries ").append(writeAdmissionController.activeTenantBudgets()).append('\n')
                .append("# TYPE ull_matcher_http_tenant_write_anonymous_total counter\n")
                .append("ull_matcher_http_tenant_write_anonymous_total ").append(writeAdmissionController.anonymousTenantRequests()).append('\n');

        builder.append("# TYPE ull_matcher_http_route_requests_total counter\n")
                .append("# TYPE ull_matcher_http_route_overload_total counter\n")
                .append("# TYPE ull_matcher_http_route_timeout_total counter\n")
                .append("# TYPE ull_matcher_http_route_inflight gauge\n")
                .append("# TYPE ull_matcher_http_route_saturation gauge\n");
        http.routeBudgets().forEach(routeBudget -> appendRouteMetrics(builder, routeBudget));

        builder.append("# TYPE ull_matcher_http_endpoint_requests_total counter\n")
                .append("# TYPE ull_matcher_http_endpoint_overload_total counter\n")
                .append("# TYPE ull_matcher_http_endpoint_timeouts_total counter\n")
                .append("# TYPE ull_matcher_http_endpoint_failures_total counter\n")
                .append("# TYPE ull_matcher_http_endpoint_inflight gauge\n")
                .append("# TYPE ull_matcher_http_endpoint_max_inflight gauge\n")
                .append("# TYPE ull_matcher_http_endpoint_budget_saturation gauge\n")
                .append("# TYPE ull_matcher_http_endpoint_route_saturation_share gauge\n")
                .append("# TYPE ull_matcher_http_endpoint_duration_ms_sum counter\n")
                .append("# TYPE ull_matcher_http_endpoint_duration_ms_max gauge\n")
                .append("# TYPE ull_matcher_http_endpoint_duration_bucket counter\n");
        http.endpointStats().forEach((endpoint, stats) -> appendEndpointMetrics(builder, endpoint, stats));
    }

    private static void appendRouteMetrics(StringBuilder builder, RouteBudget routeBudget) {
        int inflight = routeBudget.maxConcurrentRequests() - routeBudget.slots().availablePermits();
        double saturation = routeBudget.maxConcurrentRequests() == 0
                ? 0.0d
                : ((double) inflight) / routeBudget.maxConcurrentRequests();
        builder.append("ull_matcher_http_route_requests_total{route=\"").append(routeBudget.name()).append("\"} ")
                .append(routeBudget.requestCount().get()).append('\n')
                .append("ull_matcher_http_route_overload_total{route=\"").append(routeBudget.name()).append("\"} ")
                .append(routeBudget.overloadCount().get()).append('\n')
                .append("ull_matcher_http_route_timeout_total{route=\"").append(routeBudget.name()).append("\"} ")
                .append(routeBudget.timeoutCount().get()).append('\n')
                .append("ull_matcher_http_route_inflight{route=\"").append(routeBudget.name()).append("\"} ")
                .append(inflight).append('\n')
                .append("ull_matcher_http_route_saturation{route=\"").append(routeBudget.name()).append("\"} ")
                .append(saturation).append('\n');
    }

    private static void appendEndpointMetrics(StringBuilder builder, String endpoint, EndpointStats stats) {
        double saturationShare = stats.routeMaxConcurrentRequests() == 0
                ? 0.0d
                : ((double) stats.inflight().get()) / stats.routeMaxConcurrentRequests();
        double endpointBudgetSaturation = stats.endpointMaxConcurrentRequests() == 0
                ? 0.0d
                : ((double) stats.inflight().get()) / stats.endpointMaxConcurrentRequests();
        builder.append("ull_matcher_http_endpoint_requests_total{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.requestCount().get()).append('\n')
                .append("ull_matcher_http_endpoint_overload_total{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.overloadCount().get()).append('\n')
                .append("ull_matcher_http_endpoint_timeouts_total{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.timeoutCount().get()).append('\n')
                .append("ull_matcher_http_endpoint_failures_total{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.failureCount().get()).append('\n')
                .append("ull_matcher_http_endpoint_inflight{endpoint=\"").append(endpoint).append("\",route=\"")
                .append(stats.routeName()).append("\"} ").append(stats.inflight().get()).append('\n')
                .append("ull_matcher_http_endpoint_max_inflight{endpoint=\"").append(endpoint).append("\",route=\"")
                .append(stats.routeName()).append("\"} ").append(stats.maxInflight().get()).append('\n')
                .append("ull_matcher_http_endpoint_budget_saturation{endpoint=\"").append(endpoint).append("\",route=\"")
                .append(stats.routeName()).append("\"} ").append(endpointBudgetSaturation).append('\n')
                .append("ull_matcher_http_endpoint_route_saturation_share{endpoint=\"").append(endpoint).append("\",route=\"")
                .append(stats.routeName()).append("\"} ").append(saturationShare).append('\n')
                .append("ull_matcher_http_endpoint_duration_ms_sum{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.durationSumMillis().get()).append('\n')
                .append("ull_matcher_http_endpoint_duration_ms_max{endpoint=\"").append(endpoint).append("\"} ")
                .append(stats.durationMaxMillis().get()).append('\n');
        long[] bucketBoundaries = HttpRouteMetrics.ENDPOINT_LATENCY_BUCKETS_MILLIS;
        for (int i = 0; i < bucketBoundaries.length; i++) {
            builder.append("ull_matcher_http_endpoint_duration_bucket{endpoint=\"")
                    .append(endpoint)
                    .append("\",le=\"")
                    .append(bucketBoundaries[i])
                    .append("\"} ")
                    .append(stats.bucketCounts()[i].get())
                    .append('\n');
        }
        builder.append("ull_matcher_http_endpoint_duration_bucket{endpoint=\"")
                .append(endpoint)
                .append("\",le=\"+Inf\"} ")
                .append(stats.requestCount().get())
                .append('\n');
    }

    private static String escapeLabelValue(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    record HttpMetrics(
            int maxConcurrentRequests,
            int requestAvailablePermits,
            int executorQueueDepth,
            int executorQueueCapacity,
            long globalOverloadCount,
            WriteAdmissionController writeAdmissionController,
            List<RouteBudget> routeBudgets,
            Map<String, EndpointStats> endpointStats
    ) {}
}
