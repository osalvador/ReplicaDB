package org.replicadb.server.security.api;

import jakarta.validation.Valid;
import org.replicadb.server.job.api.PageRequestParams;
import org.replicadb.server.job.api.PageResponse;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.crypto.password.PasswordEncoder;
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
@RequestMapping("/api/v1/users")
@PreAuthorize("hasRole('ADMIN')")
public class UserController {

    private final AppUserRepository repository;
    private final PasswordEncoder passwordEncoder;

    public UserController(AppUserRepository repository, PasswordEncoder passwordEncoder) {
        this.repository = repository;
        this.passwordEncoder = passwordEncoder;
    }

    @PostMapping
    public ResponseEntity<UserResponse> create(@Valid @RequestBody UserRequest request) {
        if (repository.findByUsername(request.username()).isPresent()) {
            throw new IllegalStateException("Username is already in use");
        }
        AppUser user = new AppUser(null, request.username(), passwordEncoder.encode(request.password()),
                request.role(), true, null, null);
        try {
            AppUser persisted = repository.insert(user);
            return ResponseEntity.created(URI.create("/api/v1/users/" + persisted.id()))
                    .body(UserResponse.from(persisted));
        } catch (DuplicateKeyException exception) {
            throw new IllegalStateException("Username is already in use");
        }
    }

    @GetMapping
    public PageResponse<UserResponse> list(@RequestParam(required = false) Integer page,
                                           @RequestParam(required = false) Integer size) {
        PageRequestParams params = PageRequestParams.of(page, size);
        return new PageResponse<>(repository.findPage(params.page(), params.size()).stream()
                .map(UserResponse::from)
                .toList(), params.page(), params.size(), repository.count());
    }

    @GetMapping("/{id}")
    public UserResponse get(@PathVariable UUID id) {
        return UserResponse.from(findUser(id));
    }

    @PutMapping("/{id}")
    public UserResponse update(@PathVariable UUID id, @Valid @RequestBody UserRequest.RoleUpdate request) {
        AppUser existing = findUser(id);
        AppUser replacement = new AppUser(existing.id(), existing.username(), existing.passwordHash(),
                request.role(), request.enabled(), existing.createdAt(), existing.updatedAt());
        return UserResponse.from(repository.update(replacement));
    }

    @PutMapping("/{id}/password")
    public UserResponse updatePassword(@PathVariable UUID id,
                                       @Valid @RequestBody UserRequest.PasswordUpdate request) {
        AppUser existing = findUser(id);
        AppUser replacement = new AppUser(existing.id(), existing.username(),
                passwordEncoder.encode(request.newPassword()), existing.role(), existing.enabled(),
                existing.createdAt(), existing.updatedAt());
        return UserResponse.from(repository.update(replacement));
    }

    private AppUser findUser(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("AppUser not found: " + id));
    }
}
