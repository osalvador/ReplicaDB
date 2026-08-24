package org.replicadb.server.job.execution;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ToolOptions;
import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunExecutionHandleTest {

    @Test
    void exposesClaimIdentityOptionsAndCancellationContext() throws Exception {
        LeaseToken leaseToken = LeaseToken.generate();
        JobRun run = claimedRun(leaseToken);
        ToolOptions options = options();

        RunExecutionHandle handle = new RunExecutionHandle(run, options);

        assertEquals(run.id(), handle.runId());
        assertEquals(leaseToken, handle.leaseToken());
        assertSame(options, handle.toolOptions());
        assertSame(options.getExecutionContext(), handle.cancellationContext());
        assertFalse(handle.cancellationContext().isCancellationRequested());
    }

    @Test
    void requestsCancellationOnItsLocalContext() throws Exception {
        RunExecutionHandle handle = new RunExecutionHandle(claimedRun(LeaseToken.generate()), options());

        handle.requestCancellation();

        assertTrue(handle.cancellationContext().isCancellationRequested());
    }

    @Test
    void rejectsAnUnclaimedRun() throws Exception {
        JobRun run = new JobRun(UUID.randomUUID(), UUID.randomUUID(), null, JobRunStatus.PENDING, 1,
                null, null, null, Instant.now(), null, null, null, null, null, null, null);

        assertThrows(IllegalArgumentException.class, () -> new RunExecutionHandle(run, options()));
    }

    @Test
    void registryRegistrationAndRemovalAreIdempotent() throws Exception {
        ActiveRunRegistry registry = new ActiveRunRegistry();
        RunExecutionHandle handle = new RunExecutionHandle(claimedRun(LeaseToken.generate()), options());

        assertTrue(registry.register(handle));
        assertFalse(registry.register(handle));
        assertSame(handle, registry.find(handle.runId()).orElseThrow());
        assertTrue(registry.remove(handle.runId(), handle));
        assertFalse(registry.remove(handle.runId(), handle));
        assertTrue(registry.find(handle.runId()).isEmpty());
    }

    @Test
    void missingHandleDoesNotSignalCancellation() {
        ActiveRunRegistry registry = new ActiveRunRegistry();

        assertFalse(registry.requestCancellation(UUID.randomUUID()));
    }

    @Test
    void registryCancellationSignalsTheRegisteredHandle() throws Exception {
        ActiveRunRegistry registry = new ActiveRunRegistry();
        RunExecutionHandle handle = new RunExecutionHandle(claimedRun(LeaseToken.generate()), options());
        registry.register(handle);

        assertTrue(registry.requestCancellation(handle.runId()));
        assertTrue(handle.cancellationContext().isCancellationRequested());
    }

    private static JobRun claimedRun(LeaseToken leaseToken) {
        return new JobRun(UUID.randomUUID(), UUID.randomUUID(), null, JobRunStatus.RUNNING, 1,
                "worker", Instant.now().plusSeconds(300), Instant.now(), Instant.now(),
            Instant.now(), null, null, null, null, null, null, Instant.now(), leaseToken);
    }

    private static ToolOptions options() throws Exception {
        return new ToolOptions(new String[]{
                "--source-connect", "jdbc:sqlite:source.db",
                "--sink-connect", "jdbc:sqlite:sink.db"
        });
    }
}