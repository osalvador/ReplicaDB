package org.replicadb.server.job.api;

import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Schema;
import org.replicadb.server.job.port.JobRunStore;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Schema(description = "Permission-aware dashboard aggregates for one explicit effective time window.")
public record DashboardSummaryResponse(
    @Schema(description = "Inclusive effective window start in UTC.", format = "date-time") Instant from,
    @Schema(description = "Exclusive effective window end in UTC.", format = "date-time") Instant to,
    @Schema(description = "Jobs visible to the current identity.", minimum = "0") long totalJobs,
    @Schema(description = "Runs currently active, distinct from terminal outcomes.", minimum = "0") long activeRuns,
    @Schema(description = "Runs counted in the effective window.", minimum = "0") long totalRuns,
    @Schema(description = "Successfully completed runs in the window.", minimum = "0") long succeededRuns,
    @Schema(description = "Failed runs in the window.", minimum = "0") long failedRuns,
    @Schema(description = "Rows reported by counted runs; not a correctness guarantee.", minimum = "0") long rowsProcessed,
    @Schema(description = "Mean execution duration in milliseconds; zero when no completed duration exists.", minimum = "0") long averageDurationMillis,
    @Schema(description = "Mean queue-to-start latency in milliseconds; zero when no started run exists.", minimum = "0") long averageLatencyMillis,
    @ArraySchema(arraySchema = @Schema(description = "Time-bucketed run outcomes.")) List<DashboardOutcomePoint> outcomes,
    @ArraySchema(arraySchema = @Schema(description = "Per-job throughput and timing summaries.")) List<DashboardJobPerformance> jobPerformance) {

    public static DashboardSummaryResponse from(Instant from, Instant to, long totalJobs,
                                                 JobRunStore.DashboardRunSummary summary) {
        return new DashboardSummaryResponse(from, to, totalJobs, summary.activeRuns(), summary.totalRuns(),
                summary.succeededRuns(), summary.failedRuns(), summary.rowsProcessed(),
                summary.averageDurationMillis(), summary.averageLatencyMillis(),
                summary.outcomeBuckets().stream().map(DashboardOutcomePoint::from).toList(),
                summary.jobPerformance().stream().map(DashboardJobPerformance::from).toList());
    }
}

@Schema(description = "Run outcome counts for one dashboard time bucket.")
record DashboardOutcomePoint(
    @Schema(description = "Bucket start timestamp in UTC.", format = "date-time") Instant bucket,
    @Schema(description = "Successful runs in this bucket.", minimum = "0") long succeeded,
    @Schema(description = "Failed runs in this bucket.", minimum = "0") long failed,
    @Schema(description = "Active runs in this bucket.", minimum = "0") long active) {
    static DashboardOutcomePoint from(JobRunStore.OutcomeBucket bucket) {
        return new DashboardOutcomePoint(bucket.bucket(), bucket.succeeded(), bucket.failed(), bucket.active());
    }
}

@Schema(description = "Dashboard aggregate for one visible job.")
record DashboardJobPerformance(
    @Schema(description = "Job identifier.", format = "uuid") UUID jobId,
    @Schema(description = "Job display name.") String jobName,
    @Schema(description = "Counted runs.", minimum = "0") long runCount,
    @Schema(description = "Rows reported by counted runs.", minimum = "0") long rowsProcessed,
    @Schema(description = "Mean execution duration in milliseconds.", minimum = "0") long averageDurationMillis,
    @Schema(description = "Mean queue-to-start latency in milliseconds.", minimum = "0") long averageLatencyMillis) {
    static DashboardJobPerformance from(JobRunStore.JobPerformance performance) {
        return new DashboardJobPerformance(performance.jobId(), performance.jobName(), performance.runCount(),
                performance.rowsProcessed(), performance.averageDurationMillis(), performance.averageLatencyMillis());
    }
}
