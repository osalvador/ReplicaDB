package org.replicadb.server.job.api;

import org.replicadb.server.job.port.JobDefinitionStore;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.security.JobAccessService;
import org.springframework.context.annotation.Profile;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@RestController
@Profile("api")
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private final JobRunStore jobRunStore;
    private final JobDefinitionStore jobDefinitionStore;
    private final JobAccessService jobAccessService;

    public DashboardController(JobRunStore jobRunStore, JobDefinitionStore jobDefinitionStore,
                               JobAccessService jobAccessService) {
        this.jobRunStore = jobRunStore;
        this.jobDefinitionStore = jobDefinitionStore;
        this.jobAccessService = jobAccessService;
    }

    @GetMapping("/summary")
    public DashboardSummaryResponse summary(
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            Authentication authentication) {
        Instant effectiveTo = to == null ? Instant.now() : to;
        Instant effectiveFrom = from == null ? effectiveTo.minus(Duration.ofHours(24)) : from;
        if (!effectiveFrom.isBefore(effectiveTo)) {
            throw new IllegalArgumentException("Dashboard range must have a start before its end");
        }
        Optional<Set<UUID>> visibleJobIds = jobAccessService.visibleJobIds(authentication);
        Set<UUID> restriction = visibleJobIds.orElse(null);
        JobRunStore.DashboardRunSummary summary = jobRunStore.summarizeDashboard(
                effectiveFrom, effectiveTo, restriction);
        return DashboardSummaryResponse.from(effectiveFrom, effectiveTo,
                jobDefinitionStore.count(restriction), summary);
    }
}
