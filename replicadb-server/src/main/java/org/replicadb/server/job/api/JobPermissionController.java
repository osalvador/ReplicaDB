package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import org.replicadb.server.security.JobAccessService;
import org.replicadb.server.security.domain.JobPermission;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.replicadb.server.security.persistence.JobPermissionRepository;
import org.springframework.http.ResponseEntity;
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

@RestController
@RequestMapping("/api/v1/jobs/{jobDefinitionId}/permissions")
public class JobPermissionController {

    private final JobAccessService jobAccessService;
    private final JobPermissionRepository jobPermissionRepository;
    private final AppUserRepository appUserRepository;

    public JobPermissionController(JobAccessService jobAccessService,
                                   JobPermissionRepository jobPermissionRepository,
                                   AppUserRepository appUserRepository) {
        this.jobAccessService = jobAccessService;
        this.jobPermissionRepository = jobPermissionRepository;
        this.appUserRepository = appUserRepository;
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
        findUser(userId);
        jobPermissionRepository.revokeAll(jobDefinitionId, userId);
        request.permissions().forEach(permission ->
                jobPermissionRepository.grant(jobDefinitionId, userId, permission));
        return grouped(jobPermissionRepository.findByJobDefinitionId(jobDefinitionId)).stream()
                .filter(response -> response.userId().equals(userId))
                .findFirst()
                .orElseThrow();
    }

    @DeleteMapping("/{userId}")
    public ResponseEntity<Void> delete(@PathVariable UUID jobDefinitionId,
                                       @PathVariable UUID userId,
                                       Authentication authentication) {
        requireEdit(authentication, jobDefinitionId);
        findUser(userId);
        jobPermissionRepository.revokeAll(jobDefinitionId, userId);
        return ResponseEntity.noContent().build();
    }

    private void requireEdit(Authentication authentication, UUID jobDefinitionId) {
        jobAccessService.require(authentication, jobDefinitionId, JobPermissionType.EDIT);
    }

    private void findUser(UUID userId) {
        appUserRepository.findById(userId)
                .orElseThrow(() -> new NoSuchElementException("AppUser not found: " + userId));
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
