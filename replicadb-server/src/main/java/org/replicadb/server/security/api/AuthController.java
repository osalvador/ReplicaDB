package org.replicadb.server.security.api;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.validation.Valid;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.replicadb.server.security.auth.LoginAttemptService;
import org.replicadb.server.security.auth.LoginAttemptReservation;
import org.replicadb.server.security.auth.TooManyAttemptsException;
import org.springframework.http.ResponseEntity;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@Profile("api")
@RequestMapping("/api/v1/auth")
@Tag(name = "Authentication", description = "CSRF bootstrap, login, logout, and current session identity.")
public class AuthController {

    private final AuthenticationManager authenticationManager;
    private final LoginAttemptService loginAttemptService;
    private final SecurityContextRepository securityContextRepository;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    public AuthController(AuthenticationManager authenticationManager,
                          LoginAttemptService loginAttemptService,
                          SecurityContextRepository securityContextRepository,
                          AuditService auditService,
                          AuditActorResolver auditActorResolver) {
        this.authenticationManager = authenticationManager;
        this.loginAttemptService = loginAttemptService;
        this.securityContextRepository = securityContextRepository;
        this.auditService = auditService;
        this.auditActorResolver = auditActorResolver;
    }

    @PostMapping("/login")
        @Operation(operationId = "login", summary = "Create an authenticated session",
            description = "Authenticates a user and establishes the server-owned session. Login is public and exempt from CSRF, but database-backed throttling limits repeated failures by account and source address.",
            security = {})
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Authenticated identity returned and session established"),
            @ApiResponse(responseCode = "400", ref = "#/components/responses/BadRequestProblem"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "429", ref = "#/components/responses/TooManyRequestsProblem")
        })
    public UserIdentityResponse login(@Valid @RequestBody LoginRequest request,
                          @Parameter(hidden = true) HttpServletRequest httpRequest,
                          @Parameter(hidden = true) HttpServletResponse httpResponse) {
        String remoteAddress = httpRequest.getRemoteAddr();
        LoginAttemptReservation reservation;
        try {
            reservation = loginAttemptService.checkAllowed(request.username(), remoteAddress);
        } catch (TooManyAttemptsException exception) {
            auditService.record(auditActorResolver.forAttemptedLogin(request.username(), remoteAddress),
                    AuditAction.LOGIN_FAILED, AuditResourceType.SESSION, request.username(),
                    AuditOutcome.FAILURE, Map.of("reason", "THROTTLED"));
            throw exception;
        }

        Authentication authentication;
        try {
            authentication = authenticationManager.authenticate(
                    UsernamePasswordAuthenticationToken.unauthenticated(request.username(), request.password()));
        } catch (AuthenticationException exception) {
            loginAttemptService.recordFailure(reservation);
            auditService.record(auditActorResolver.forAttemptedLogin(request.username(), remoteAddress),
                    AuditAction.LOGIN_FAILED, AuditResourceType.SESSION, request.username(),
                    AuditOutcome.FAILURE);
            throw exception;
        }

        loginAttemptService.recordSuccess(reservation);
        AuditActor actor = auditActorResolver.resolve(authentication);
        auditService.record(actor, AuditAction.LOGIN_SUCCEEDED, AuditResourceType.SESSION,
                actor.username(), AuditOutcome.SUCCESS);
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(authentication);
        SecurityContextHolder.setContext(context);
        securityContextRepository.saveContext(context, httpRequest, httpResponse);
        return UserIdentityResponse.from(authentication);
    }

    @GetMapping("/csrf")
        @Operation(operationId = "getCsrfToken", summary = "Initialize CSRF protection",
            description = "Initializes the XSRF-TOKEN cookie and returns the framework header and parameter names. Call this public endpoint before protected state-changing requests.",
            security = {})
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "CSRF contract initialized"),
            @ApiResponse(responseCode = "500", ref = "#/components/responses/InternalServerErrorProblem")
        })
        public CsrfTokenResponse csrf(@Parameter(hidden = true) HttpServletRequest request) {
        CsrfToken csrfToken = (CsrfToken) request.getAttribute(CsrfToken.class.getName());
        if (csrfToken == null) {
            throw new IllegalStateException("CSRF token was not initialized");
        }
        return new CsrfTokenResponse(csrfToken.getHeaderName(), csrfToken.getParameterName(), csrfToken.getToken());
    }

    @PostMapping("/logout")
        @Operation(operationId = "logout", summary = "End the authenticated session",
            description = "Invalidates the current server session and clears its security context. The session cookie and CSRF header are required.",
            security = @SecurityRequirement(name = "sessionCookie"))
        @ApiResponses({
            @ApiResponse(responseCode = "204", description = "Session ended"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem"),
            @ApiResponse(responseCode = "403", ref = "#/components/responses/ForbiddenProblem")
        })
        public ResponseEntity<Void> logout(@Parameter(hidden = true) HttpServletRequest request,
                           Authentication authentication) {
        AuditActor actor = auditActorResolver.resolve(authentication);
        HttpSession session = request.getSession(false);
        if (session != null) {
            session.invalidate();
        }
        SecurityContextHolder.clearContext();
        auditService.record(actor, AuditAction.LOGOUT, AuditResourceType.SESSION,
                actor.username(), AuditOutcome.SUCCESS);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/me")
        @Operation(operationId = "getCurrentIdentity", summary = "Get the current identity",
            description = "Returns the user identifier, username, and global role associated with the current session.",
            security = @SecurityRequirement(name = "sessionCookie"))
        @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Current identity returned"),
            @ApiResponse(responseCode = "401", ref = "#/components/responses/UnauthorizedProblem")
        })
    public UserIdentityResponse me(Authentication authentication) {
        return UserIdentityResponse.from(authentication);
    }

    @JsonPropertyOrder({"headerName", "parameterName", "token"})
    @Schema(description = "CSRF bootstrap values issued by Spring Security.")
    public record CsrfTokenResponse(
            @Schema(description = "HTTP header name expected on protected mutations.", example = "X-XSRF-TOKEN") String headerName,
            @Schema(description = "Framework request parameter name.") String parameterName,
            @Schema(description = "Opaque CSRF value that must match the XSRF-TOKEN cookie.", accessMode = Schema.AccessMode.READ_ONLY) String token) {
    }
}
