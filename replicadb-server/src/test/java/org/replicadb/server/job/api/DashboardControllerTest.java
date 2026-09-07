package org.replicadb.server.job.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.replicadb.server.job.port.JobDefinitionStore;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.security.JobAccessService;
import org.springframework.security.core.Authentication;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DashboardControllerTest {

    @Mock
    private JobRunStore jobRunStore;

    @Mock
    private JobDefinitionStore jobDefinitionStore;

    @Mock
    private JobAccessService jobAccessService;

    @Mock
    private Authentication authentication;

    @Test
    void defaultsToTwentyFourHoursAndRestrictsAggregatesToVisibleJobs() {
        Set<UUID> visibleJobIds = Set.of(UUID.randomUUID());
        when(jobAccessService.visibleJobIds(authentication)).thenReturn(Optional.of(visibleJobIds));
        when(jobDefinitionStore.count(visibleJobIds)).thenReturn(1L);
        when(jobRunStore.summarizeDashboard(
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(visibleJobIds)))
                .thenReturn(new JobRunStore.DashboardRunSummary(0, 0, 0, 0, 0, 0, 0,
                        List.of(), List.of()));
        DashboardController controller = new DashboardController(
                jobRunStore, jobDefinitionStore, jobAccessService);

        Instant before = Instant.now();
        DashboardSummaryResponse response = controller.summary(null, null, authentication);
        Instant after = Instant.now();

        assertFalse(response.to().isBefore(before));
        assertFalse(response.to().isAfter(after));
        assertEquals(Duration.ofHours(24), Duration.between(response.from(), response.to()));
        assertEquals(1L, response.totalJobs());

        ArgumentCaptor<Set<UUID>> restriction = ArgumentCaptor.forClass(Set.class);
        verify(jobRunStore).summarizeDashboard(
                org.mockito.ArgumentMatchers.eq(response.from()),
                org.mockito.ArgumentMatchers.eq(response.to()), restriction.capture());
        assertEquals(visibleJobIds, restriction.getValue());
    }
}
