package org.replicadb.server.job.persistence;

import org.replicadb.server.job.domain.JobRun;
import org.replicadb.server.job.domain.JobRunStatus;
import org.replicadb.server.job.domain.LeaseToken;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

public final class JobRunRowMapper implements RowMapper<JobRun> {

    @Override
    public JobRun mapRow(ResultSet resultSet, int rowNum) throws SQLException {
        UUID leaseTokenValue = resultSet.getObject("lease_token", UUID.class);
        return new JobRun(
                resultSet.getObject("id", UUID.class),
                resultSet.getObject("job_definition_id", UUID.class),
                resultSet.getObject("previous_run_id", UUID.class),
                JobRunStatus.valueOf(resultSet.getString("status")),
                resultSet.getInt("attempt"),
                resultSet.getString("executor_identity"),
                toInstant(resultSet.getTimestamp("lease_until")),
                toInstant(resultSet.getTimestamp("heartbeat_at")),
                toInstant(resultSet.getTimestamp("created_at")),
                toInstant(resultSet.getTimestamp("started_at")),
                toInstant(resultSet.getTimestamp("finished_at")),
                nullableLong(resultSet, "rows_processed"),
                nullableLong(resultSet, "duration_millis"),
                resultSet.getString("committed_watermark"),
                resultSet.getString("error_message"),
                resultSet.getString("cancellation_warning"),
                toInstant(resultSet.getTimestamp("available_at")),
                leaseTokenValue == null ? null : new LeaseToken(leaseTokenValue));
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static Long nullableLong(ResultSet resultSet, String column) throws SQLException {
        long value = resultSet.getLong(column);
        return resultSet.wasNull() ? null : value;
    }
}
