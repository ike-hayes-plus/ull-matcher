/*
 * Copyright 2026 ull-matcher authors
 * SPDX-License-Identifier: Apache-2.0
 */
package io.github.ike.ullmatcher.server.api;

import java.util.concurrent.Semaphore;

/**
 * Per-endpoint concurrency budget.
 *
 * @param name metric key
 * @param maxConcurrentRequests configured ceiling
 * @param slots remaining permits
 */
record HttpEndpointBudget(String name, int maxConcurrentRequests, Semaphore slots) {
    static HttpEndpointBudget create(String name, int maxConcurrentRequests) {
        return new HttpEndpointBudget(name, maxConcurrentRequests, new Semaphore(maxConcurrentRequests));
    }
}
