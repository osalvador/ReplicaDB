package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.port.DataSourcePermissionStore;
import org.replicadb.server.job.port.ManagedDataSourceStore;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.DataSourcePermission;
import org.replicadb.server.security.domain.DataSourcePermissionType;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
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
@Profile("api")
@PreAuthorize("hasRole('ADMIN')")
@RequestMapping("/api/v1/datasources/{datasourceId}/permissions")
public class DatasourcePermissionController {

    private final ManagedDataSourceStore dataSourceStore;
    private final DataSourcePermissionStore permissionStore;
    private final AppUserRepository userRepository;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    public DatasourcePermissionController(ManagedDataSourceStore dataSourceStore,
                                          DataSourcePermissionStore permissionStore,
                                          AppUserRepository userRepository,
                                          AuditService auditService,
                                          AuditActorResolver auditActorResolver) {
        this.dataSourceStore = dataSourceStore;
        this.permissionStore = permissionStore;
        this.userRepository = userRepository;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
    }

    @GetMapping
    public List<DatasourcePermissionResponse> list(@PathVariable UUID datasourceId) {
        requireDatasource(datasourceId);
        Map<UUID, Set<DataSourcePermissionType>> grouped = new LinkedHashMap<>();
        for (DataSourcePermission permission : permissionStore.findByDatasourceId(datasourceId)) {
            grouped.computeIfAbsent(permission.userId(), ignored -> new LinkedHashSet<>())
                    .add(permission.permission());
        }
        List<DatasourcePermissionResponse> responses = new ArrayList<>();
        for (Map.Entry<UUID, Set<DataSourcePermissionType>> entry : grouped.entrySet()) {
            responses.add(response(entry.getKey(), entry.getValue()));
        }
        return responses;
    }

    @PutMapping("/{userId}")
    @Transactional
    public DatasourcePermissionResponse replace(@PathVariable UUID datasourceId,
                                                @PathVariable UUID userId,
                                                @Valid @RequestBody DatasourcePermissionRequest request,
                                                Authentication authentication) {
        requireDatasource(datasourceId);
        findUser(userId);
        permissionStore.replace(datasourceId, userId, request.permissions());
        auditService.record(auditActorResolver.resolve(authentication),
                AuditAction.DATASOURCE_PERMISSION_REPLACED, AuditResourceType.DATASOURCE,
                datasourceId.toString(), AuditOutcome.SUCCESS,
                Map.of("targetUserId", userId.toString(),
                        "permissionCategoryCount", Integer.toString(request.permissions().size())));
        return response(userId, request.permissions());
    }

    @DeleteMapping("/{userId}")
    @Transactional
    public ResponseEntity<Void> revoke(@PathVariable UUID datasourceId,
                                       @PathVariable UUID userId,
                                       Authentication authentication) {
        requireDatasource(datasourceId);
        findUser(userId);
        permissionStore.revokeAll(datasourceId, userId);
        auditService.record(auditActorResolver.resolve(authentication),
                AuditAction.DATASOURCE_PERMISSION_REVOKED, AuditResourceType.DATASOURCE,
                datasourceId.toString(), AuditOutcome.SUCCESS,
                Map.of("targetUserId", userId.toString()));
        return ResponseEntity.noContent().build();
    }

    private void requireDatasource(UUID datasourceId) {
        dataSourceStore.findSummaryById(datasourceId)
                .orElseThrow(() -> new NoSuchElementException("ManagedDataSource not found: " + datasourceId));
    }

    private AppUser findUser(UUID userId) {
        return userRepository.findById(userId)
                .orElseThrow(() -> new NoSuchElementException("AppUser not found: " + userId));
    }

    private DatasourcePermissionResponse response(UUID userId, Set<DataSourcePermissionType> permissions) {
        return new DatasourcePermissionResponse(userId, findUser(userId).username(), Set.copyOf(permissions));
    }
}
