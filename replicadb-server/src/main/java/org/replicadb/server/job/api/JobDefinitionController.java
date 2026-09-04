package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import jakarta.validation.groups.Default;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.domain.DataSourceCapabilities;
import org.replicadb.server.job.domain.DataSourceCapabilityCatalog;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;
import org.replicadb.server.job.port.ManagedDataSourceStore;
import org.replicadb.server.job.port.JobDefinitionStore;
import org.replicadb.server.job.port.JobRunStore;
import org.replicadb.server.security.JobAccessService;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.security.DataSourceAccessService;
import org.replicadb.server.security.persistence.JobPermissionRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.context.annotation.Profile;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.replicadb.manager.DataSourceType;

@RestController
@Profile("api")
@RequestMapping("/api/v1/jobs")
public class JobDefinitionController {

    private final JobDefinitionStore repository;
    private final JobDefinitionMapper mapper;
    private final JobAccessService jobAccessService;
    private final JobPermissionRepository jobPermissionRepository;
    private final ManagedDataSourceStore dataSourceStore;
    private final DataSourceCapabilityCatalog capabilityCatalog;
    private final DataSourceAccessService dataSourceAccessService;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;
    private final JobRunStore jobRunStore;
    private final org.replicadb.server.job.execution.QuartzScheduleService quartzScheduleService;

    public JobDefinitionController(JobDefinitionStore repository, JobDefinitionMapper mapper,
                                   JobAccessService jobAccessService,
                                   JobPermissionRepository jobPermissionRepository,
                                   ManagedDataSourceStore dataSourceStore,
                                   DataSourceCapabilityCatalog capabilityCatalog,
                                   DataSourceAccessService dataSourceAccessService,
                                   AuditService auditService,
                                   AuditActorResolver auditActorResolver,
                                   JobRunStore jobRunStore,
                                   org.replicadb.server.job.execution.QuartzScheduleService quartzScheduleService) {
        this.repository = repository;
        this.mapper = mapper;
        this.jobAccessService = jobAccessService;
        this.jobPermissionRepository = jobPermissionRepository;
        this.dataSourceStore = dataSourceStore;
        this.capabilityCatalog = capabilityCatalog;
        this.dataSourceAccessService = dataSourceAccessService;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
        this.jobRunStore = jobRunStore;
        this.quartzScheduleService = quartzScheduleService;
    }

    @PostMapping
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Transactional
    public ResponseEntity<JobDefinitionResponse> create(
            @Validated({Default.class, JobDefinitionRequest.Create.class})
            @RequestBody JobDefinitionRequest request,
            Authentication authentication) {
            ManagedDataSourceSummary sourceDatasource = validateBinding(request.sourceDatasourceId(),
                DataSourceType.SOURCE, authentication, null, true, request.sourceDatasourceUseEnabled(), true);
            ManagedDataSourceSummary sinkDatasource = validateBinding(request.sinkDatasourceId(),
                DataSourceType.SINK, authentication, null, true, request.sinkDatasourceUseEnabled(), true);
            JobDefinition definition = mapper.toDefinition(request, null, request.name(), null, null);
            validateMode(definition, sourceDatasource, sinkDatasource);
            JobDefinition persisted = repository.insert(definition);
        if (!jobAccessService.isAdmin(authentication)) {
            jobPermissionRepository.grantAll(persisted.id(), jobAccessService.currentUserId(authentication));
        }
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.JOB_CREATED,
            AuditResourceType.JOB_DEFINITION, persisted.id().toString(), AuditOutcome.SUCCESS,
            auditDetail(persisted));
        return ResponseEntity.created(URI.create("/api/v1/jobs/" + persisted.id()))
            .body(mapper.toResponse(persisted, sourceDatasource, sinkDatasource));
    }

    @GetMapping
    public PageResponse<JobDefinitionResponse> list(
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size,
            Authentication authentication) {
        PageRequestParams params = PageRequestParams.of(page, size);
        Optional<Set<UUID>> visibleJobIds = jobAccessService.visibleJobIds(authentication);
        Set<UUID> restriction = visibleJobIds.orElse(null);
        return new PageResponse<>(repository.findPage(params.page(), params.size(), restriction).stream()
            .map(definition -> response(definition, authentication))
                .toList(), params.page(), params.size(), repository.count(restriction));
    }

    @GetMapping("/{id}")
    public JobDefinitionResponse get(@PathVariable UUID id, Authentication authentication) {
        jobAccessService.require(authentication, id, JobPermissionType.VIEW);
        return response(findDefinition(id), authentication);
    }

    @PutMapping("/{id}")
    @Transactional
    public JobDefinitionResponse update(@PathVariable UUID id, @Valid @RequestBody JobDefinitionRequest request,
                                         Authentication authentication) {
        jobAccessService.require(authentication, id, JobPermissionType.EDIT);
        JobDefinition existing = repository.findByIdForUpdate(id)
                .orElseThrow(() -> new NoSuchElementException("JobDefinition not found: " + id));
        if (request.name() != null && !existing.name().equals(request.name())) {
            throw new IllegalArgumentException("name cannot be changed");
        }
        ManagedDataSourceSummary sourceDatasource = validateBinding(request.sourceDatasourceId(),
            DataSourceType.SOURCE, authentication, existing.sourceDatasourceId(),
            existing.sourceDatasourceUseEnabled(), request.sourceDatasourceUseEnabled(), false);
        ManagedDataSourceSummary sinkDatasource = validateBinding(request.sinkDatasourceId(),
            DataSourceType.SINK, authentication, existing.sinkDatasourceId(),
            existing.sinkDatasourceUseEnabled(), request.sinkDatasourceUseEnabled(), false);
        JobDefinition replacement = mapper.toDefinition(request, existing.id(), existing.name(),
            existing.createdAt(), existing.updatedAt(), existing.retryPolicy(), existing.mode(),
            existing.sourceDatasourceUseEnabled(), existing.sinkDatasourceUseEnabled());
        validateMode(replacement, sourceDatasource, sinkDatasource);
        JobDefinition persisted = repository.update(replacement);
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.JOB_UPDATED,
            AuditResourceType.JOB_DEFINITION, persisted.id().toString(), AuditOutcome.SUCCESS,
            auditDetail(persisted));
        auditBindingChanges(existing, persisted, sourceDatasource, sinkDatasource, authentication);
        return mapper.toResponse(persisted, sourceDatasource, sinkDatasource);
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    @Transactional
        @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Job deleted"),
            @ApiResponse(responseCode = "403", description = "Administrator access required"),
            @ApiResponse(responseCode = "404", description = "Job definition not found"),
            @ApiResponse(responseCode = "409", description = "Job has an active run or cannot be unscheduled")
        })
    public ResponseEntity<Void> delete(@PathVariable UUID id, Authentication authentication) {
        JobDefinition definition = repository.findByIdForUpdate(id)
                .orElseThrow(() -> new NoSuchElementException("JobDefinition not found: " + id));
        if (jobRunStore.hasActiveRun(id)) {
            throw new IllegalStateException("Cannot delete JobDefinition with an active run: " + id);
        }
        quartzScheduleService.unschedule(id);
        JobDefinitionStore.DeleteResult result = repository.delete(definition.id());
        if (result.status() != JobDefinitionStore.DeleteStatus.DELETED) {
            throw new NoSuchElementException("JobDefinition not found: " + id);
        }
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.JOB_DELETED,
                AuditResourceType.JOB_DEFINITION, id.toString(), AuditOutcome.SUCCESS,
                Map.of("name", result.jobName()));
        return ResponseEntity.noContent().build();
    }

    private JobDefinition findDefinition(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("JobDefinition not found: " + id));
    }

    private static Map<String, String> auditDetail(JobDefinition definition) {
        return Map.of(
                "name", definition.name(),
                "mode", definition.mode().getModeText(),
                "jobs", Integer.toString(definition.jobs()),
                "sourceTable", definition.sourceTable() == null ? "<query>" : definition.sourceTable(),
                "sinkTable", definition.sinkTable());
    }

    private JobDefinitionResponse response(JobDefinition definition, Authentication authentication) {
        return mapper.toResponse(definition, visibleDatasource(definition.sourceDatasourceId(), authentication),
                visibleDatasource(definition.sinkDatasourceId(), authentication));
    }

    private ManagedDataSourceSummary visibleDatasource(UUID datasourceId, Authentication authentication) {
        if (!dataSourceAccessService.canView(authentication, datasourceId)) {
            return null;
        }
        return dataSourceStore.findSummaryById(datasourceId).orElse(null);
    }

    private ManagedDataSourceSummary validateBinding(UUID datasourceId, DataSourceType role,
                                                     Authentication authentication, UUID existingId,
                                                     boolean existingUseEnabled, Boolean requestedUseEnabled,
                                                     boolean creating) {
        ManagedDataSourceSummary dataSource = dataSourceStore.findSummaryById(datasourceId)
                .orElseThrow(() -> new NoSuchElementException("ManagedDataSource not found: " + datasourceId));
        DataSourceCapabilities capabilities = capabilityCatalog.forType(dataSource.connectorType());
        boolean roleSupported = role == DataSourceType.SOURCE
                ? capabilities.supportsSource() : capabilities.supportsSink();
        if (!roleSupported) {
            throw new IllegalArgumentException("Datasource cannot be used as a " + role.name().toLowerCase());
        }
        boolean useEnabled = requestedUseEnabled == null ? (creating || existingUseEnabled) : requestedUseEnabled;
        boolean bindingChanged = creating || !datasourceId.equals(existingId);
        if ((bindingChanged || (!existingUseEnabled && useEnabled))
                && !dataSourceAccessService.canUse(authentication, datasourceId)) {
            throw new AccessDeniedException("Access denied");
        }
        return dataSource;
    }

    private void validateMode(JobDefinition definition, ManagedDataSourceSummary sourceDatasource,
                              ManagedDataSourceSummary sinkDatasource) {
        DataSourceCapabilities sourceCapabilities = capabilityCatalog.forType(sourceDatasource.connectorType());
        DataSourceCapabilities sinkCapabilities = capabilityCatalog.forType(sinkDatasource.connectorType());
        if (!sourceCapabilities.supports(DataSourceType.SOURCE, definition.mode())) {
            throw new IllegalArgumentException("Source datasource does not support mode "
                    + definition.mode().getModeText());
        }
        if (!sinkCapabilities.supports(DataSourceType.SINK, definition.mode())) {
            throw new IllegalArgumentException("Sink datasource does not support mode "
                    + definition.mode().getModeText());
        }
        if (definition.sourceQuery() != null && !sourceCapabilities.sourceQuery()) {
            throw new IllegalArgumentException("Source datasource does not support query input");
        }
        if ((sourceCapabilities.singleJobOnly() || sinkCapabilities.singleJobOnly()) && definition.jobs() != 1) {
            throw new IllegalArgumentException("Selected datasource supports only one job");
        }
    }

    private void auditBindingChanges(JobDefinition previous, JobDefinition current,
                                     ManagedDataSourceSummary sourceDatasource,
                                     ManagedDataSourceSummary sinkDatasource,
                                     Authentication authentication) {
        auditBindingChange(previous.sourceDatasourceId(), current.sourceDatasourceId(),
                previous.sourceDatasourceUseEnabled(), current.sourceDatasourceUseEnabled(),
                "source", sourceDatasource, current.id(), authentication);
        auditBindingChange(previous.sinkDatasourceId(), current.sinkDatasourceId(),
                previous.sinkDatasourceUseEnabled(), current.sinkDatasourceUseEnabled(),
                "sink", sinkDatasource, current.id(), authentication);
    }

    private void auditBindingChange(UUID previousId, UUID currentId, boolean previousEnabled,
                                    boolean currentEnabled, String side,
                                    ManagedDataSourceSummary dataSource, UUID jobId,
                                    Authentication authentication) {
        if (!previousId.equals(currentId)) {
            auditService.record(auditActorResolver.resolve(authentication),
                    AuditAction.JOB_DATASOURCE_BINDING_REPLACED, AuditResourceType.JOB_DEFINITION,
                    jobId.toString(), AuditOutcome.SUCCESS,
                    bindingDetail(side, currentId, dataSource));
        }
        if (previousEnabled != currentEnabled) {
            auditService.record(auditActorResolver.resolve(authentication),
                    currentEnabled ? AuditAction.JOB_DATASOURCE_BINDING_ENABLED
                            : AuditAction.JOB_DATASOURCE_BINDING_DISABLED,
                    AuditResourceType.JOB_DEFINITION, jobId.toString(), AuditOutcome.SUCCESS,
                    bindingDetail(side, currentId, dataSource));
        }
    }

    private static Map<String, String> bindingDetail(String side, UUID datasourceId,
                                                      ManagedDataSourceSummary dataSource) {
        return Map.of("side", side, "datasourceId", datasourceId.toString(),
                "datasourceName", dataSource.name(),
                "connectorType", dataSource.connectorType().getWireValue());
    }
}
