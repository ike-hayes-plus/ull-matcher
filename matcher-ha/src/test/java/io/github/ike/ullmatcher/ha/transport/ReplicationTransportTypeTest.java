package io.github.ike.ullmatcher.ha.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Whether the gRPC replication server has to be started is decided from the transport type alone;
 * getting it wrong either starts a server AERON does not use or leaves GRPC without its control plane.
 */
final class ReplicationTransportTypeTest {
    @Test
    void grpcNeedsTheGrpcReplicationServer() {
        assertTrue(ReplicationTransportType.GRPC.requiresGrpcReplicationServer());
    }

    @Test
    void fullAeronRunsWithoutTheGrpcReplicationServer() {
        assertFalse(ReplicationTransportType.AERON.requiresGrpcReplicationServer());
    }

    @Test
    void exactlyOneTransportTypeIsGrpcFree() {
        long grpcFree = 0L;
        for (ReplicationTransportType type : ReplicationTransportType.values()) {
            if (!type.requiresGrpcReplicationServer()) {
                grpcFree++;
            }
        }

        assertEquals(1L, grpcFree);
        assertEquals(2, ReplicationTransportType.values().length);
        assertEquals(ReplicationTransportType.AERON, ReplicationTransportType.valueOf("AERON"));
    }
}
