package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import jakarta.validation.groups.Default;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.security.JobAccessService;
import org.replicadb.server.security.domain.JobPermissionType;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.replicadb.server.security.persistence.JobPermissionRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobDefinitionController {

    private final JobDefinitionRepository repository;
    private final JobDefinitionMapper mapper;
    private final JobAccessService jobAccessService;
    private final JobPermissionRepository jobPermissionRepository;

    public JobDefinitionController(JobDefinitionRepository repository, JobDefinitionMapper mapper,
                                   JobAccessService jobAccessService,
                                   JobPermissionRepository jobPermissionRepository) {
        this.repository = repository;
        this.mapper = mapper;
        this.jobAccessService = jobAccessService;
        this.jobPermissionRepository = jobPermissionRepository;
    }

    @PostMapping
    @PreAuthorize("hasAnyRole('ADMIN','OPERATOR')")
    @Transactional
    public ResponseEntity<JobDefinitionResponse> create(
            @Validated({Default.class, JobDefinitionRequest.Create.class})
            @RequestBody JobDefinitionRequest request,
            Authentication authentication) {
        JobDefinition persisted = repository.insert(
                mapper.toDefinition(request, null, request.name(), null, null));
        if (!jobAccessService.isAdmin(authentication)) {
            jobPermissionRepository.grantAll(persisted.id(), jobAccessService.currentUserId(authentication));
        }
        return ResponseEntity.created(URI.create("/api/v1/jobs/" + persisted.id()))
                .body(mapper.toResponse(persisted));
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
                .map(mapper::toResponse)
                .toList(), params.page(), params.size(), repository.count(restriction));
    }

    @GetMapping("/{id}")
    public JobDefinitionResponse get(@PathVariable UUID id, Authentication authentication) {
        jobAccessService.require(authentication, id, JobPermissionType.VIEW);
        return mapper.toResponse(findDefinition(id));
    }

    @PutMapping("/{id}")
    public JobDefinitionResponse update(@PathVariable UUID id, @Valid @RequestBody JobDefinitionRequest request,
                                         Authentication authentication) {
        jobAccessService.require(authentication, id, JobPermissionType.EDIT);
        JobDefinition existing = findDefinition(id);
        if (request.name() != null && !existing.name().equals(request.name())) {
            throw new IllegalArgumentException("name cannot be changed");
        }
        JobDefinition replacement = mapper.toDefinition(request, existing.id(), existing.name(),
                existing.createdAt(), existing.updatedAt());
        return mapper.toResponse(repository.update(replacement));
    }

    private JobDefinition findDefinition(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("JobDefinition not found: " + id));
    }
}
