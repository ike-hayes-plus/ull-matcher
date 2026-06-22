package io.github.ike.ullmatcher.server.api;

import io.github.ike.ullmatcher.server.bootstrap.WriteAdmissionPolicyConfig;
import io.undertow.server.HttpServerExchange;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

final class WriteAdmissionController {
    private static final Admission NOOP_ADMISSION = () -> {};
    private static final long TENANT_BUDGET_IDLE_EVICT_NANOS = 5L * 60L * 1_000_000_000L;

    private final String shardKey;
    private final WriteAdmissionPolicyConfig config;
    private final Semaphore shardSlots;
    private final TokenBucket shardRateLimiter;
    private final Map<String, Integer> tenantWeightOverrides;
    private final AtomicLong shardInflight = new AtomicLong();
    private final AtomicLong shardOverloadCount = new AtomicLong();
    private final AtomicLong tenantOverloadCount = new AtomicLong();
    private final AtomicLong shardRateLimitedCount = new AtomicLong();
    private final AtomicLong tenantRateLimitedCount = new AtomicLong();
    private final AtomicLong anonymousTenantRequests = new AtomicLong();
    private final ConcurrentHashMap<String, TenantBudget> tenantBudgets = new ConcurrentHashMap<>();

    WriteAdmissionController(String shardKey, int shardMaxConcurrentRequests, int tenantMaxConcurrentRequests, String tenantHeaderName) {
        this(shardKey, new WriteAdmissionPolicyConfig(
                shardMaxConcurrentRequests,
                tenantMaxConcurrentRequests,
                tenantHeaderName,
                0.0d,
                0,
                0.0d,
                0,
                1,
                "",
                "X-Ull-Tenant-Priority"
        ));
    }

    WriteAdmissionController(String shardKey, WriteAdmissionPolicyConfig config) {
        this.shardKey = Objects.requireNonNull(shardKey, "shardKey");
        this.config = Objects.requireNonNull(config, "config");
        this.shardSlots = config.shardMaxConcurrentRequests() > 0 ? new Semaphore(config.shardMaxConcurrentRequests()) : null;
        this.shardRateLimiter = TokenBucket.create(config.shardRateLimitPerSecond(), config.shardRateBurst());
        this.tenantWeightOverrides = parseTenantWeightOverrides(config.tenantWeightOverrides());
    }

    Admission acquireForSubmit(HttpServerExchange exchange, long userId) {
        String tenantKey = tenantKey(exchange, "user:" + userId);
        return acquire(new AdmissionContext(tenantKey, priority(exchange), tenantWeight(tenantKey)));
    }

    Admission acquireForCancel(HttpServerExchange exchange) {
        String tenantKey = tenantKey(exchange, null);
        if (tenantKey == null) {
            anonymousTenantRequests.incrementAndGet();
        }
        return acquire(new AdmissionContext(tenantKey, priority(exchange), tenantWeight(tenantKey)));
    }

    long shardInflight() {
        return shardInflight.get();
    }

    String shardKey() {
        return shardKey;
    }

    long shardOverloadCount() {
        return shardOverloadCount.get();
    }

    double shardSaturation() {
        if (config.shardMaxConcurrentRequests() <= 0) {
            return 0.0d;
        }
        return (double) shardInflight.get() / config.shardMaxConcurrentRequests();
    }

    long tenantOverloadCount() {
        return tenantOverloadCount.get();
    }

    long shardRateLimitedCount() {
        return shardRateLimitedCount.get();
    }

    long tenantRateLimitedCount() {
        return tenantRateLimitedCount.get();
    }

    long activeTenantBudgets() {
        evictIdleTenantBudgets();
        return tenantBudgets.values().stream().filter(budget -> budget.inflight.get() > 0L).count();
    }

    long anonymousTenantRequests() {
        return anonymousTenantRequests.get();
    }

    double shardRateLimitPerSecond() {
        return config.shardRateLimitPerSecond();
    }

    double tenantRateLimitPerSecond() {
        return config.tenantRateLimitPerSecond();
    }

    int tenantDefaultWeight() {
        return config.tenantDefaultWeight();
    }

    private Admission acquire(AdmissionContext context) {
        boolean shardAcquired = false;
        if (shardSlots != null) {
            if (!shardSlots.tryAcquire()) {
                shardOverloadCount.incrementAndGet();
                throw new OverloadedException("shard write budget exhausted; shard=" + shardKey + " limit=" + config.shardMaxConcurrentRequests());
            }
            shardAcquired = true;
            shardInflight.incrementAndGet();
        }
        TenantBudget budget = null;
        if (context.tenantKey != null && config.tenantMaxConcurrentRequests() > 0) {
            budget = tenantBudgets.computeIfAbsent(context.tenantKey, ignored -> new TenantBudget(tenantRateLimiter(context.weight)));
            budget.touch();
            if (!budget.tryAcquireSlot(tenantConcurrentLimit(context.weight))) {
                tenantOverloadCount.incrementAndGet();
                cleanupTenantBudget(context.tenantKey, budget);
                if (shardAcquired) {
                    releaseShard();
                }
                throw new OverloadedException("tenant write budget exhausted; tenant=" + context.tenantKey
                        + " limit=" + tenantConcurrentLimit(context.weight) + " weight=" + context.weight);
            }
        }

        double requestCost = context.priority.requestCost();
        if (shardRateLimiter != null && !shardRateLimiter.tryConsume(requestCost)) {
            shardRateLimitedCount.incrementAndGet();
            if (budget != null) {
                releaseTenant(context.tenantKey, budget);
            }
            if (shardAcquired) {
                releaseShard();
            }
            throw new OverloadedException("shard write rate limit exceeded; shard=" + shardKey
                    + " rate=" + config.shardRateLimitPerSecond() + "/s burst=" + effectiveBurst(config.shardRateLimitPerSecond(), config.shardRateBurst())
                    + " priority=" + context.priority.name());
        }
        if (budget != null && budget.rateLimiter != null && !budget.rateLimiter.tryConsume(requestCost)) {
            tenantRateLimitedCount.incrementAndGet();
            releaseTenant(context.tenantKey, budget);
            if (shardAcquired) {
                releaseShard();
            }
            throw new OverloadedException("tenant write rate limit exceeded; tenant=" + context.tenantKey
                    + " rate=" + scaledRate(config.tenantRateLimitPerSecond(), context.weight) + "/s burst="
                    + scaledBurst(config.tenantRateLimitPerSecond(), config.tenantRateBurst(), context.weight)
                    + " weight=" + context.weight + " priority=" + context.priority.name());
        }
        if (!shardAcquired) {
            if (budget == null) {
                return NOOP_ADMISSION;
            }
            TenantBudget tenantBudget = budget;
            return () -> releaseTenant(context.tenantKey, tenantBudget);
        }
        if (budget == null) {
            return this::releaseShard;
        }
        TenantBudget tenantBudget = budget;
        return () -> {
            try {
                releaseTenant(context.tenantKey, tenantBudget);
            } finally {
                releaseShard();
            }
        };
    }

    private String tenantKey(HttpServerExchange exchange, String fallback) {
        String headerValue = exchange.getRequestHeaders().getFirst(config.tenantAdmissionHeader());
        if (headerValue != null) {
            String normalized = headerValue.trim();
            if (!normalized.isEmpty()) {
                return normalized;
            }
        }
        return fallback;
    }

    private void releaseTenant(String tenantKey, TenantBudget budget) {
        long remaining = budget.releaseSlot();
        if (remaining == 0L) {
            cleanupTenantBudget(tenantKey, budget);
        }
    }

    private void cleanupTenantBudget(String tenantKey, TenantBudget budget) {
        budget.touch();
        if (budget.inflight.get() == 0L && (budget.rateLimiter == null || budget.idleFor(System.nanoTime()) >= TENANT_BUDGET_IDLE_EVICT_NANOS)) {
            tenantBudgets.remove(tenantKey, budget);
        }
    }

    private void evictIdleTenantBudgets() {
        long nowNanos = System.nanoTime();
        tenantBudgets.forEach((tenantKey, budget) -> {
            if (budget.inflight.get() == 0L && budget.idleFor(nowNanos) >= TENANT_BUDGET_IDLE_EVICT_NANOS) {
                tenantBudgets.remove(tenantKey, budget);
            }
        });
    }

    private void releaseShard() {
        shardInflight.decrementAndGet();
        shardSlots.release();
    }

    @FunctionalInterface
    interface Admission extends AutoCloseable {
        @Override
        void close();
    }

    private int tenantWeight(String tenantKey) {
        if (tenantKey == null) {
            return config.tenantDefaultWeight();
        }
        return tenantWeightOverrides.getOrDefault(tenantKey, config.tenantDefaultWeight());
    }

    private AdmissionPriority priority(HttpServerExchange exchange) {
        String headerValue = exchange.getRequestHeaders().getFirst(config.tenantPriorityHeader());
        if (headerValue == null || headerValue.isBlank()) {
            return AdmissionPriority.NORMAL;
        }
        return AdmissionPriority.parse(headerValue);
    }

    private int tenantConcurrentLimit(int weight) {
        if (config.tenantMaxConcurrentRequests() <= 0) {
            return 0;
        }
        long scaled = (long) config.tenantMaxConcurrentRequests() * Math.max(1, weight);
        if (config.shardMaxConcurrentRequests() > 0) {
            scaled = Math.min(scaled, config.shardMaxConcurrentRequests());
        }
        return (int) Math.max(1L, scaled);
    }

    private TokenBucket tenantRateLimiter(int weight) {
        if (config.tenantRateLimitPerSecond() <= 0.0d) {
            return null;
        }
        return TokenBucket.create(
                scaledRate(config.tenantRateLimitPerSecond(), weight),
                scaledBurst(config.tenantRateLimitPerSecond(), config.tenantRateBurst(), weight)
        );
    }

    private static double scaledRate(double baseRate, int weight) {
        return baseRate * Math.max(1, weight);
    }

    private static int scaledBurst(double baseRate, int configuredBurst, int weight) {
        return effectiveBurst(baseRate, configuredBurst) * Math.max(1, weight);
    }

    private static int effectiveBurst(double ratePerSecond, int configuredBurst) {
        if (ratePerSecond <= 0.0d) {
            return 0;
        }
        return configuredBurst > 0 ? configuredBurst : Math.max(1, (int) Math.ceil(ratePerSecond));
    }

    private static Map<String, Integer> parseTenantWeightOverrides(String raw) {
        if (raw.isBlank()) {
            return Map.of();
        }
        ConcurrentHashMap<String, Integer> parsed = new ConcurrentHashMap<>();
        for (String entry : raw.split(",")) {
            String normalized = entry.trim();
            if (normalized.isEmpty()) {
                continue;
            }
            int separator = normalized.indexOf('=');
            if (separator <= 0 || separator == normalized.length() - 1) {
                throw new IllegalArgumentException("invalid tenant weight override entry: " + normalized);
            }
            String tenant = normalized.substring(0, separator).trim();
            int weight = Integer.parseInt(normalized.substring(separator + 1).trim());
            if (tenant.isEmpty() || weight <= 0) {
                throw new IllegalArgumentException("invalid tenant weight override entry: " + normalized);
            }
            parsed.put(tenant, weight);
        }
        return Map.copyOf(parsed);
    }

    private static final class TenantBudget {
        private final AtomicLong inflight = new AtomicLong();
        private final TokenBucket rateLimiter;
        private volatile long lastTouchedNanos = System.nanoTime();

        private TenantBudget(TokenBucket rateLimiter) {
            this.rateLimiter = rateLimiter;
        }

        private boolean tryAcquireSlot(int limit) {
            for (;;) {
                long current = inflight.get();
                if (current >= limit) {
                    return false;
                }
                if (inflight.compareAndSet(current, current + 1)) {
                    touch();
                    return true;
                }
            }
        }

        private long releaseSlot() {
            touch();
            return inflight.decrementAndGet();
        }

        private void touch() {
            lastTouchedNanos = System.nanoTime();
        }

        private long idleFor(long nowNanos) {
            return nowNanos - lastTouchedNanos;
        }
    }

    private record AdmissionContext(String tenantKey, AdmissionPriority priority, int weight) {}

    private enum AdmissionPriority {
        LOW(2.0d),
        NORMAL(1.0d),
        HIGH(0.5d),
        CRITICAL(0.25d);

        private final double requestCost;

        AdmissionPriority(double requestCost) {
            this.requestCost = requestCost;
        }

        private double requestCost() {
            return requestCost;
        }

        private static AdmissionPriority parse(String value) {
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return NORMAL;
            }
        }
    }

    private static final class TokenBucket {
        private final double ratePerSecond;
        private final double maxTokens;
        private double tokens;
        private long lastRefillNanos;

        private TokenBucket(double ratePerSecond, int burst) {
            this.ratePerSecond = ratePerSecond;
            this.maxTokens = burst;
            this.tokens = burst;
            this.lastRefillNanos = System.nanoTime();
        }

        private static TokenBucket create(double ratePerSecond, int burst) {
            if (ratePerSecond <= 0.0d) {
                return null;
            }
            return new TokenBucket(ratePerSecond, effectiveBurst(ratePerSecond, burst));
        }

        private synchronized boolean tryConsume(double cost) {
            refill(System.nanoTime());
            if (tokens + 1e-9 < cost) {
                return false;
            }
            tokens -= cost;
            return true;
        }

        private void refill(long nowNanos) {
            long elapsed = nowNanos - lastRefillNanos;
            if (elapsed <= 0L) {
                return;
            }
            double replenished = (elapsed / 1_000_000_000.0d) * ratePerSecond;
            tokens = Math.min(maxTokens, tokens + replenished);
            lastRefillNanos = nowNanos;
        }
    }
}
