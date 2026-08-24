package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.security.JobAccessService;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.JobPermission;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.replicadb.server.security.persistence.JobPermissionRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@Profile("api")
@RequestMapping("/api/v1/jobs/{jobDefinitionId}/permissions")
public class JobPermissionController {

    private final JobAccessService jobAccessService;
    private final JobPermissionRepository jobPermissionRepository;
    private final AppUserRepository appUserRepository;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    public JobPermissionController(JobAccessService jobAccessService,
                                   JobPermissionRepository jobPermissionRepository,
                                   AppUserRepository appUserRepository,
                                   AuditService auditService,
                                   AuditActorResolver auditActorResolver) {
        this.jobAccessService = jobAccessService;
        this.jobPermissionRepository = jobPermissionRepository;
        this.appUserRepository = appUserRepository;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
    }

    @GetMapping
    public List<JobPermissionResponse> list(@PathVariable UUID jobDefinitionId,
                                            Authentication authentication) {
        requireEdit(authentication, jobDefinitionId);
        return grouped(jobPermissionRepository.findByJobDefinitionId(jobDefinitionId));
    }

    @PutMapping("/{userId}")
    @Transactional
    public JobPermissionResponse replace(@PathVariable UUID jobDefinitionId,
                                         @PathVariable UUID userId,
                                         @Valid @RequestBody JobPermissionRequest request,
                                         Authentication authentication) {
        requireEdit(authentication, jobDefinitionId);
                        AppUser targetUser = findUser(userId);
        jobPermissionRepository.revokeAll(jobDefinitionId, userId);
        request.permissions().forEach(permission ->
                jobPermissionRepository.grant(jobDefinitionId, userId, permission));
                        JobPermissionResponse response = new JobPermissionResponse(userId, targetUser.username(),
                            Set.copyOf(request.permissions()));
                        auditService.record(auditActorResolver.resolve(authentication), AuditAction.JOB_PERMISSION_REPLACED,
                            AuditResourceType.JOB_DEFINITION, jobDefinitionId.toString(), AuditOutcome.SUCCESS,
                            Map.of("targetUserId", userId.toString(), "permissions", permissionsDetail(request)));
                        return response;
    }

    @DeleteMapping("/{userId}")
    public ResponseEntity<Void> delete(@PathVariable UUID jobDefinitionId,
                                       @PathVariable UUID userId,
                                       Authentication authentication) {
        requireEdit(authentication, jobDefinitionId);
        findUser(userId);
        jobPermissionRepository.revokeAll(jobDefinitionId, userId);
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.JOB_PERMISSION_REVOKED,
            AuditResourceType.JOB_DEFINITION, jobDefinitionId.toString(), AuditOutcome.SUCCESS,
            Map.of("targetUserId", userId.toString()));
        return ResponseEntity.noContent().build();
    }

    private void requireEdit(Authentication authentication, UUID jobDefinitionId) {
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.EDIT);
    }

    private AppUser findUser(UUID userId) {
        return appUserRepository.findById(userId)
            .orElseThrow(() -> new NoSuchElementException("AppUser not found: " + userId));
    }

    private static String permissionsDetail(JobPermissionRequest request) {
        if (request.permissions().isEmpty()) {
            return "none";
        }
        return request.permissions().stream()
                .map(Enum::name)
                .collect(Collectors.joining(","));
    }

    private List<JobPermissionResponse> grouped(List<JobPermission> permissions) {
        Map<UUID, Set<JobPermissionType>> grouped = new LinkedHashMap<>();
        for (JobPermission permission : permissions) {
            grouped.computeIfAbsent(permission.userId(), ignored -> new LinkedHashSet<>())
                    .add(permission.permission());
        }
        List<JobPermissionResponse> responses = new ArrayList<>();
        for (Map.Entry<UUID, Set<JobPermissionType>> entry : grouped.entrySet()) {
            String username = appUserRepository.findById(entry.getKey())
                    .orElseThrow(() -> new NoSuchElementException("AppUser not found: " + entry.getKey()))
                    .username();
            responses.add(new JobPermissionResponse(entry.getKey(), username, Set.copyOf(entry.getValue())));
        }
        return responses;
    }
}
