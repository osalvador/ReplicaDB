package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.job.domain.ConnectorType;
import org.replicadb.server.job.domain.DataSourceCapabilities;
import org.replicadb.server.job.domain.DataSourceCapabilityCatalog;
import org.replicadb.server.job.domain.ManagedDataSource;
import org.replicadb.server.job.domain.ManagedDataSourceSummary;
import org.replicadb.server.job.port.ManagedDataSourceStore;
import org.replicadb.server.security.DataSourceAccessService;
import org.replicadb.server.security.secret.EncryptedSecurityBundle;
import org.replicadb.server.security.secret.SecretProtectionService;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@RestController
@Profile("api")
@RequestMapping("/api/v1/datasources")
public class DatasourceController {

    private final ManagedDataSourceStore repository;
    private final DatasourceMapper mapper;
    private final DataSourceCapabilityCatalog capabilityCatalog;
    private final DataSourceAccessService accessService;
    private final SecretProtectionService protectionService;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    public DatasourceController(ManagedDataSourceStore repository,
                                DatasourceMapper mapper,
                                DataSourceCapabilityCatalog capabilityCatalog,
                                DataSourceAccessService accessService,
                                SecretProtectionService protectionService,
                                AuditService auditService,
                                AuditActorResolver auditActorResolver) {
        this.repository = repository;
        this.mapper = mapper;
        this.capabilityCatalog = capabilityCatalog;
        this.accessService = accessService;
        this.protectionService = protectionService;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
    }

    @PostMapping
    @PreAuthorize("hasRole('ADMIN')")
    @Transactional
    public ResponseEntity<DatasourceResponse> create(@Valid @RequestBody DatasourceRequest request,
                                                     Authentication authentication) {
        UUID id = UUID.randomUUID();
        ConnectorType connectorType = mapper.connectorType(request);
        DataSourceCapabilities capabilities = capabilityCatalog.forType(connectorType);
        Map<String, String> security = mapper.mergedSecurity(request, Map.of());
        validateConnector(connectorType, security.get("connect"));
        EncryptedSecurityBundle bundle = protectionService.encrypt(id, security);
        ManagedDataSource persisted = insert(request, id, security, bundle);
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.DATASOURCE_CREATED,
            AuditResourceType.DATASOURCE, persisted.id().toString(), AuditOutcome.SUCCESS,
            auditDetail(persisted, request, security));
        return ResponseEntity.created(URI.create("/api/v1/datasources/" + persisted.id()))
                .body(response(persisted, capabilities, authentication));
    }

    @GetMapping
    public PageResponse<DatasourceResponse> list(@RequestParam(required = false) Integer page,
                                                 @RequestParam(required = false) Integer size,
                                                 @RequestParam(required = false) String role,
                                                 Authentication authentication) {
        PageRequestParams params = PageRequestParams.of(page, size);
        Optional<Set<UUID>> visibleIds = accessService.visibleDatasourceIds(authentication);
        Set<ConnectorType> types = allowedTypes(role);
        Set<UUID> restriction = visibleIds.orElse(null);
        return new PageResponse<>(repository.findPage(params.page(), params.size(), restriction, types).stream()
                .map(summary -> response(summary, authentication))
                .toList(), params.page(), params.size(), repository.count(restriction, types));
    }

    @GetMapping("/{id}")
    public DatasourceResponse get(@PathVariable UUID id, Authentication authentication) {
        accessService.requireView(authentication, id);
        ManagedDataSourceSummary summary = repository.findSummaryById(id)
                .orElseThrow(() -> new NoSuchElementException("ManagedDataSource not found: " + id));
        return response(summary, authentication);
    }

    @PutMapping("/{id}")
    @Transactional
    public DatasourceResponse update(@PathVariable UUID id, @Valid @RequestBody DatasourceRequest request,
                                     Authentication authentication) {
        accessService.requireEdit(authentication, id);
        ManagedDataSource existing = repository.findById(id)
                .orElseThrow(() -> new NoSuchElementException("ManagedDataSource not found: " + id));
        Map<String, String> currentSecurity = protectionService.decrypt(id,
                protectionService.deserialize(existing.encryptedSecurity()));
        Map<String, String> security = mapper.mergedSecurity(request, currentSecurity);
        ConnectorType connectorType = mapper.connectorType(request);
        DataSourceCapabilities capabilities = capabilityCatalog.forType(connectorType);
        validateConnector(connectorType, security.get("connect"));
        EncryptedSecurityBundle bundle = protectionService.encrypt(id, security);
        ManagedDataSource replacement = mapper.toDataSource(id, request, security, bundle,
                protectionService.serialize(bundle), existing.createdAt(), existing.updatedAt());
        ManagedDataSource updated;
        try {
            updated = repository.update(replacement);
        } catch (DuplicateKeyException exception) {
            throw duplicateName();
        }
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.DATASOURCE_UPDATED,
                AuditResourceType.DATASOURCE, updated.id().toString(), AuditOutcome.SUCCESS,
                auditDetail(updated, request, security));
        return response(updated, capabilities, authentication);
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    @Transactional
    public ResponseEntity<Void> delete(@PathVariable UUID id, Authentication authentication) {
        ManagedDataSourceStore.DeleteResult result = repository.delete(id);
        if (result == ManagedDataSourceStore.DeleteResult.NOT_FOUND) {
            throw new NoSuchElementException("ManagedDataSource not found: " + id);
        }
        if (result == ManagedDataSourceStore.DeleteResult.REFERENCED) {
            throw new IllegalStateException("ManagedDataSource is referenced by "
                    + repository.countJobReferences(id) + " job(s): " + id);
        }
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.DATASOURCE_DELETED,
            AuditResourceType.DATASOURCE, id.toString(), AuditOutcome.SUCCESS);
        return ResponseEntity.noContent().build();
    }

    private DatasourceResponse response(ManagedDataSource dataSource,
                                        DataSourceCapabilities capabilities,
                                        Authentication authentication) {
        return mapper.toResponse(dataSource, capabilities, accessService.canView(authentication, dataSource.id()),
                accessService.canUse(authentication, dataSource.id()),
                accessService.canEdit(authentication, dataSource.id()));
    }

    private DatasourceResponse response(ManagedDataSourceSummary summary, Authentication authentication) {
        return mapper.toResponse(summary, capabilityCatalog.forType(summary.connectorType()),
                accessService.canView(authentication, summary.id()),
                accessService.canUse(authentication, summary.id()),
                accessService.canEdit(authentication, summary.id()));
    }

    private Set<ConnectorType> allowedTypes(String role) {
        if (role == null || role.isBlank()) {
            return null;
        }
        org.replicadb.manager.DataSourceType dataSourceType = switch (role.trim().toLowerCase(Locale.ROOT)) {
            case "source" -> org.replicadb.manager.DataSourceType.SOURCE;
            case "sink" -> org.replicadb.manager.DataSourceType.SINK;
            default -> throw new IllegalArgumentException("role must be source or sink");
        };
        return Arrays.stream(ConnectorType.values())
                .filter(type -> type != ConnectorType.CUSTOM)
                .filter(type -> capabilityCatalog.forType(type).supportsSource()
                        && dataSourceType == org.replicadb.manager.DataSourceType.SOURCE
                        || capabilityCatalog.forType(type).supportsSink()
                        && dataSourceType == org.replicadb.manager.DataSourceType.SINK)
                .collect(java.util.stream.Collectors.toSet());
    }

    private void validateConnector(ConnectorType connectorType, String connect) {
        if (connectorType == ConnectorType.CUSTOM || !connectorType.matchesConnection(connect)) {
            throw new IllegalArgumentException("connectorType does not match the datasource connection scheme");
        }
    }

    private ManagedDataSource insert(DatasourceRequest request, UUID id,
                                     Map<String, String> security,
                                     EncryptedSecurityBundle bundle) {
        try {
            return repository.insert(mapper.toDataSource(id, request, security, bundle,
                    protectionService.serialize(bundle), null, null));
        } catch (DuplicateKeyException exception) {
            throw duplicateName();
        }
    }

    private static IllegalStateException duplicateName() {
        return new IllegalStateException("Datasource name is already in use");
    }

    private static Map<String, String> auditDetail(ManagedDataSource dataSource,
                                                    DatasourceRequest request,
                                                    Map<String, String> security) {
        return Map.of(
                "name", dataSource.name(),
                "connectorType", dataSource.connectorType().getWireValue(),
                "technicalParameterCount", Integer.toString(request.technicalParams().size()),
                "securityCategoryCount", Integer.toString(security.size()),
                "securityUpdateRequested", Boolean.toString(!request.security().isEmpty()
                        || !request.clearSecurityKeys().isEmpty()));
    }
}
