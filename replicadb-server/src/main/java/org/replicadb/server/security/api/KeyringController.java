package org.replicadb.server.security.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.security.secret.KeyringAdministrationService;
import org.springframework.context.annotation.Profile;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@Profile("api")
@RequestMapping("/api/v1/keyring")
@PreAuthorize("hasRole('ADMIN')")
@Tag(name = "Keyring", description = "Keyring status and datasource re-encryption administration.")
@SecurityRequirement(name = "sessionCookie")
public class KeyringController {

    private static final int DEFAULT_BATCH_SIZE = 200;
    private static final int MAX_BATCH_SIZE = 1_000;

    private final KeyringAdministrationService service;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    public KeyringController(KeyringAdministrationService service,
                             AuditService auditService,
                             AuditActorResolver auditActorResolver) {
        this.service = service;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
    }

    @GetMapping("/status")
    @Operation(operationId = "getKeyringStatus", summary = "Get keyring status",
            description = "Returns known key versions and encrypted datasource envelope counts without key material. ADMIN is required.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Keyring status returned"),
        @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
        @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem")
    })
    public KeyringStatusResponse status() {
        KeyringAdministrationService.KeyringStatus status = service.status();
        return new KeyringStatusResponse(status.knownVersions(), status.currentVersion(),
                status.envelopes().stream()
                        .map(envelope -> new KeyringEnvelopeCountResponse(envelope.keyVersion(),
                                envelope.count(), envelope.known()))
                        .toList(), status.reencryptionRequired(), status.unknownVersionCount(),
                status.converged());
    }

    @PostMapping("/reencrypt")
    @Operation(operationId = "reencryptKeyring", summary = "Re-encrypt datasource envelopes",
            description = "Re-encrypts one bounded batch using the current key version. Repeat until remaining is zero. ADMIN and CSRF are required.")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Re-encryption batch completed"),
        @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
        @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem")
    })
    public KeyringReencryptResponse reencrypt(
            @RequestParam(required = false)
            @Schema(description = "Maximum number of envelopes to process in this request.",
                    defaultValue = "200", minimum = "1", maximum = "1000") Integer batchSize,
            Authentication authentication) {
        KeyringAdministrationService.ReencryptResult result = service.reencrypt(clampBatchSize(batchSize));
        auditService.record(auditActorResolver.resolve(authentication), AuditAction.KEYRING_REENCRYPTED,
                AuditResourceType.KEYRING, "keyring", AuditOutcome.SUCCESS,
                Map.of("reencrypted", String.valueOf(result.reencrypted()),
                        "remaining", String.valueOf(result.remaining())));
        return new KeyringReencryptResponse(result.reencrypted(), result.remaining(), result.remaining() == 0);
    }

    private static int clampBatchSize(Integer batchSize) {
        if (batchSize == null) {
            return DEFAULT_BATCH_SIZE;
        }
        return Math.max(1, Math.min(MAX_BATCH_SIZE, batchSize));
    }
}
