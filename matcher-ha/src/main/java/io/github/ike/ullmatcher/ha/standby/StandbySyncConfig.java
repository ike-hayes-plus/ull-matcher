package io.github.ike.ullmatcher.ha.standby;

/**
 * standby 同步配置。
 *
 * @param forceOnEveryCommand 每条命令后是否强制刷盘
 * @param applyToMatcher 是否立即重放到 standby 撮合器
 */
public record StandbySyncConfig(
        boolean forceOnEveryCommand,
        boolean applyToMatcher
) {
    public static StandbySyncConfig defaults() {
        return new StandbySyncConfig(true, true);
    }
}
