package io.github.ike.ullmatcher.ha.transport;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Whether the gRPC replication server has to be started is decided from the transport type alone;
 * getting it wrong either leaves the preview transport without its control plane or wastes a port.
 */
final class ReplicationTransportTypeTest {
    @Test
    void grpcAndAeronPreviewStillNeedTheGrpcReplicationServer() {
        assertTrue(ReplicationTransportType.GRPC.requiresGrpcReplicationServer());
        assertTrue(ReplicationTransportType.AERON_PREVIEW.requiresGrpcReplicationServer());
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
        assertEquals(3, ReplicationTransportType.values().length);
        assertEquals(ReplicationTransportType.AERON, ReplicationTransportType.valueOf("AERON"));
    }
}
