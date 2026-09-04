package org.replicadb.server.job.api;

import org.replicadb.server.job.port.JobRunStore;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record DashboardSummaryResponse(
        Instant from,
        Instant to,
        long totalJobs,
        long activeRuns,
        long totalRuns,
        long succeededRuns,
        long failedRuns,
        long rowsProcessed,
        long averageDurationMillis,
        long averageLatencyMillis,
        List<DashboardOutcomePoint> outcomes,
        List<DashboardJobPerformance> jobPerformance) {

    public static DashboardSummaryResponse from(Instant from, Instant to, long totalJobs,
                                                 JobRunStore.DashboardRunSummary summary) {
        return new DashboardSummaryResponse(from, to, totalJobs, summary.activeRuns(), summary.totalRuns(),
                summary.succeededRuns(), summary.failedRuns(), summary.rowsProcessed(),
                summary.averageDurationMillis(), summary.averageLatencyMillis(),
                summary.outcomeBuckets().stream().map(DashboardOutcomePoint::from).toList(),
                summary.jobPerformance().stream().map(DashboardJobPerformance::from).toList());
    }
}

record DashboardOutcomePoint(Instant bucket, long succeeded, long failed, long active) {
    static DashboardOutcomePoint from(JobRunStore.OutcomeBucket bucket) {
        return new DashboardOutcomePoint(bucket.bucket(), bucket.succeeded(), bucket.failed(), bucket.active());
    }
}

record DashboardJobPerformance(UUID jobId, String jobName, long runCount, long rowsProcessed,
                               long averageDurationMillis, long averageLatencyMillis) {
    static DashboardJobPerformance from(JobRunStore.JobPerformance performance) {
        return new DashboardJobPerformance(performance.jobId(), performance.jobName(), performance.runCount(),
                performance.rowsProcessed(), performance.averageDurationMillis(), performance.averageLatencyMillis());
    }
}
