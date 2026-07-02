package io.github.ike.ullmatcher.server.api;

import java.util.List;

record NewOrderBatchRequest(List<NewOrderRequest> orders, String ack) {
}
