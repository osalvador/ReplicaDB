package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import org.replicadb.server.job.domain.JobSchedule;
import org.replicadb.server.job.execution.QuartzScheduleService;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.job.persistence.JobScheduleRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.NoSuchElementException;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/jobs/{jobDefinitionId}/schedule")
@Validated
public class JobScheduleController {

    private final JobDefinitionRepository jobDefinitionRepository;
    private final JobScheduleRepository jobScheduleRepository;
    private final QuartzScheduleService quartzScheduleService;

    public JobScheduleController(JobDefinitionRepository jobDefinitionRepository,
                                 JobScheduleRepository jobScheduleRepository,
                                 QuartzScheduleService quartzScheduleService) {
        this.jobDefinitionRepository = jobDefinitionRepository;
        this.jobScheduleRepository = jobScheduleRepository;
        this.quartzScheduleService = quartzScheduleService;
    }

    @PutMapping
    public JobScheduleResponse upsert(@PathVariable UUID jobDefinitionId,
                                      @Valid @RequestBody JobScheduleRequest request) {
        findDefinition(jobDefinitionId);
        String timeZone = request.timeZone() == null || request.timeZone().isBlank()
                ? "UTC" : request.timeZone();
        JobSchedule schedule = new JobSchedule(
                jobDefinitionId, request.cronExpression(), timeZone, request.enabled(), null, null);
        JobSchedule persisted = jobScheduleRepository.upsert(schedule);
        quartzScheduleService.schedule(persisted);
        return response(persisted);
    }

    @GetMapping
    public JobScheduleResponse get(@PathVariable UUID jobDefinitionId) {
        JobSchedule schedule = jobScheduleRepository.findByJobDefinitionId(jobDefinitionId)
                .orElseThrow(() -> new NoSuchElementException(
                        "JobSchedule not found for job definition: " + jobDefinitionId));
        return response(schedule);
    }

    @DeleteMapping
    public ResponseEntity<Void> delete(@PathVariable UUID jobDefinitionId) {
        jobScheduleRepository.delete(jobDefinitionId);
        quartzScheduleService.unschedule(jobDefinitionId);
        return ResponseEntity.noContent().build();
    }

    private void findDefinition(UUID jobDefinitionId) {
        jobDefinitionRepository.findById(jobDefinitionId)
                .orElseThrow(() -> new NoSuchElementException("JobDefinition not found: " + jobDefinitionId));
    }

    private JobScheduleResponse response(JobSchedule schedule) {
        return JobScheduleResponse.from(schedule,
                quartzScheduleService.nextFireTime(schedule.jobDefinitionId()).orElse(null));
    }
}
