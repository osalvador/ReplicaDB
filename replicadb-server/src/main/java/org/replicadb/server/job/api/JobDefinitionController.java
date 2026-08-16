package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import jakarta.validation.groups.Default;
import org.replicadb.server.job.domain.JobDefinition;
import org.replicadb.server.job.persistence.JobDefinitionRepository;
import org.springframework.http.ResponseEntity;
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
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobDefinitionController {

    private final JobDefinitionRepository repository;
    private final JobDefinitionMapper mapper;

    public JobDefinitionController(JobDefinitionRepository repository, JobDefinitionMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    @PostMapping
    public ResponseEntity<JobDefinitionResponse> create(
            @Validated({Default.class, JobDefinitionRequest.Create.class})
            @RequestBody JobDefinitionRequest request) {
        JobDefinition persisted = repository.insert(
                mapper.toDefinition(request, null, request.name(), null, null));
        return ResponseEntity.created(URI.create("/api/v1/jobs/" + persisted.id()))
                .body(mapper.toResponse(persisted));
    }

    @GetMapping
    public PageResponse<JobDefinitionResponse> list(
            @RequestParam(required = false) Integer page,
            @RequestParam(required = false) Integer size) {
        PageRequestParams params = PageRequestParams.of(page, size);
        return new PageResponse<>(repository.findPage(params.page(), params.size()).stream()
                .map(mapper::toResponse)
                .toList(), params.page(), params.size(), repository.count());
    }

    @GetMapping("/{id}")
    public JobDefinitionResponse get(@PathVariable UUID id) {
        return mapper.toResponse(findDefinition(id));
    }

    @PutMapping("/{id}")
    public JobDefinitionResponse update(@PathVariable UUID id, @Valid @RequestBody JobDefinitionRequest request) {
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
