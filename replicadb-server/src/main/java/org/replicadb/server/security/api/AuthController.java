package org.replicadb.server.security.api;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
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
    public UserIdentityResponse login(@Valid @RequestBody LoginRequest request,
                                      HttpServletRequest httpRequest,
                                      HttpServletResponse httpResponse) {
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
    public CsrfTokenResponse csrf(HttpServletRequest request) {
        CsrfToken csrfToken = (CsrfToken) request.getAttribute(CsrfToken.class.getName());
        if (csrfToken == null) {
            throw new IllegalStateException("CSRF token was not initialized");
        }
        return new CsrfTokenResponse(csrfToken.getHeaderName(), csrfToken.getParameterName(), csrfToken.getToken());
    }

    @PostMapping("/logout")
    public ResponseEntity<Void> logout(HttpServletRequest request, Authentication authentication) {
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
    public UserIdentityResponse me(Authentication authentication) {
        return UserIdentityResponse.from(authentication);
    }

    @JsonPropertyOrder({"headerName", "parameterName", "token"})
    public record CsrfTokenResponse(String headerName, String parameterName, String token) {
    }
}
