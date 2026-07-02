package io.github.ike.ullmatcher.server.engine;

import io.github.ike.ullmatcher.hft.SubmitResult;

import java.util.List;

/**
 * 对外暴露的提交状态视图。
 *
 * @param submissionId 服务端分配的提交编号
 * @param idempotencyKey 幂等键
 * @param operationType 操作类型
 * @param userId 用户编号
 * @param orderId 订单编号
 * @param sequence 命令序列号；尚未定序时为 {@code 0}
 * @param phase 当前阶段
 * @param localResult 本地提交流水线结果
 * @param localDurable 是否已进入本地 WAL
 * @param replicationRequired 是否要求跨节点复制确认
 * @param replicationCommitted 是否已满足复制确认策略
 * @param totalTargets 当前复制目标数
 * @param requiredAcks 当前策略要求确认数
 * @param ackedTargets 已确认目标数
 * @param ackedNodeIds 已确认节点
 * @param failedNodeIds 最近一次复制失败节点
 * @param retryCount 复制重试次数
 * @param lastError 最近一次错误
 * @param createdAtEpochMillis 创建时间
 * @param updatedAtEpochMillis 最近更新时间
 * @param orderState 当前订单视图
 */
public record SubmissionView(
        String submissionId,
        String idempotencyKey,
        String operationType,
        Long userId,
        long orderId,
        long sequence,
        SubmissionPhase phase,
        SubmitResult localResult,
        boolean localDurable,
        boolean replicationRequired,
        boolean replicationCommitted,
        int totalTargets,
        int requiredAcks,
        int ackedTargets,
        List<String> ackedNodeIds,
        List<String> failedNodeIds,
        long retryCount,
        String lastError,
        long createdAtEpochMillis,
        long updatedAtEpochMillis,
        OrderStateView orderState
) {
}
