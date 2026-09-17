package org.replicadb.server.security.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.api.PageRequestParams;
import org.replicadb.server.job.api.PageResponse;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

@RestController
@Profile("api")
@RequestMapping("/api/v1/users")
@PreAuthorize("hasRole('ADMIN')")
@Tag(name = "Users", description = "ADMIN-managed users, global roles, account state, and password resets.")
@SecurityRequirement(name = "sessionCookie")
public class UserController {

    private final AppUserRepository repository;
    private final PasswordEncoder passwordEncoder;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    public UserController(AppUserRepository repository, PasswordEncoder passwordEncoder,
                          AuditService auditService, AuditActorResolver auditActorResolver) {
        this.repository = repository;
        this.passwordEncoder = passwordEncoder;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
    }

    @PostMapping
        @Operation(operationId = "createUser", summary = "Create a user",
            description = "Creates an enabled user with an ADMIN, OPERATOR, or VIEWER role. The password is accepted only for hashing and is never returned. ADMIN and CSRF are required.")
        @ApiResponses({
            @ApiResponse(responseCode = "201", description = "User created"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "409", ref = "#/components/responses/ConflictProblem")
        })
    public ResponseEntity<UserResponse> create(@Valid @RequestBody UserRequest request,
                                               Authentication authentication) {
        if (repository.findByUsername(request.username()).isPresent()) {
            throw new IllegalStateException("Username is already in use");
        }
        AppUser user = new AppUser(null, request.username(), passwordEncoder.encode(request.password()),
                request.role(), true, null, null);
        try {
            AppUser persisted = repository.insert(user);
            auditService.record(auditActorResolver.resolve(authentication), AuditAction.USER_CREATED,
                AuditResourceType.USER, persisted.id().toString(), AuditOutcome.SUCCESS,
                Map.of("username", persisted.username(), "role", persisted.role().name()));
            return ResponseEntity.created(URI.create("/api/v1/users/" + persisted.id()))
                    .body(UserResponse.from(persisted));
        } catch (DuplicateKeyException exception) {
            throw new IllegalStateException("Username is already in use");
        }
    }

    @GetMapping
        @Operation(operationId = "listUsers", summary = "List users",
            description = "Returns the ADMIN-visible user catalog with zero-based pagination.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Users returned"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem")
        })
        public PageResponse<UserResponse> list(
                           @Parameter(description = "Zero-based page number.", schema = @Schema(defaultValue = "0", minimum = "0"))
                           @RequestParam(required = false) Integer page,
                           @Parameter(description = "Requested page size, clamped to the range 1 through 200.", schema = @Schema(defaultValue = "50", minimum = "1", maximum = "200"))
                           @RequestParam(required = false) Integer size) {
        PageRequestParams params = PageRequestParams.of(page, size);
        return new PageResponse<>(repository.findPage(params.page(), params.size()).stream()
                .map(UserResponse::from)
                .toList(), params.page(), params.size(), repository.count());
    }

    @GetMapping("/{id}")
        @Operation(operationId = "getUser", summary = "Get a user",
            description = "Returns one user's public identity, global role, and enabled state. ADMIN is required.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "User returned"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
        public UserResponse get(
            @Parameter(description = "User identifier.", required = true) @PathVariable UUID id) {
        return UserResponse.from(findUser(id));
    }

    @PutMapping("/{id}")
        @Operation(operationId = "updateUser", summary = "Update a user's role and state",
            description = "Replaces the global role and enabled state without changing username or password. ADMIN and CSRF are required.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "User updated"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
        public UserResponse update(
                       @Parameter(description = "User identifier.", required = true) @PathVariable UUID id,
                       @Valid @RequestBody UserRequest.RoleUpdate request,
                               Authentication authentication) {
        AppUser existing = findUser(id);
        AppUser replacement = new AppUser(existing.id(), existing.username(), existing.passwordHash(),
                request.role(), request.enabled(), existing.createdAt(), existing.updatedAt());
        AppUser persisted = repository.update(replacement);
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.USER_UPDATED,
                AuditResourceType.USER, persisted.id().toString(), AuditOutcome.SUCCESS,
                Map.of("role", persisted.role().name(), "enabled", Boolean.toString(persisted.enabled())));
        return UserResponse.from(persisted);
    }

    @PutMapping("/{id}/password")
        @Operation(operationId = "updateUserPassword", summary = "Reset a user's password",
            description = "Hashes and replaces the target user's password without returning it or requiring the old value. ADMIN and CSRF are required.")
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Password reset completed"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem"),
            @ApiResponse(responseCode = "404", ref = "#/components/responses/NotFoundProblem")
        })
        public UserResponse updatePassword(
                           @Parameter(description = "User identifier.", required = true) @PathVariable UUID id,
                                       @Valid @RequestBody UserRequest.PasswordUpdate request,
                                       Authentication authentication) {
        AppUser existing = findUser(id);
        AppUser replacement = new AppUser(existing.id(), existing.username(),
                passwordEncoder.encode(request.newPassword()), existing.role(), existing.enabled(),
                existing.createdAt(), existing.updatedAt());
        AppUser persisted = repository.update(replacement);
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.USER_PASSWORD_CHANGED,
                AuditResourceType.USER, persisted.id().toString(), AuditOutcome.SUCCESS);
        return UserResponse.from(persisted);
    }

    private AppUser findUser(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("AppUser not found: " + id));
    }
}
