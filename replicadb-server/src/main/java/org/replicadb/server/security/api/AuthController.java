package org.replicadb.server.security.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.validation.Valid;
import org.replicadb.server.security.auth.LoginAttemptService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    private final AuthenticationManager authenticationManager;
    private final LoginAttemptService loginAttemptService;
    private final SecurityContextRepository securityContextRepository;

    public AuthController(AuthenticationManager authenticationManager,
                          LoginAttemptService loginAttemptService,
                          SecurityContextRepository securityContextRepository) {
        this.authenticationManager = authenticationManager;
        this.loginAttemptService = loginAttemptService;
        this.securityContextRepository = securityContextRepository;
    }

    @PostMapping("/login")
    public UserIdentityResponse login(@Valid @RequestBody LoginRequest request,
                                      HttpServletRequest httpRequest,
                                      HttpServletResponse httpResponse) {
        String remoteAddress = httpRequest.getRemoteAddr();
        loginAttemptService.checkAllowed(request.username(), remoteAddress);

        Authentication authentication;
        try {
            authentication = authenticationManager.authenticate(
                    UsernamePasswordAuthenticationToken.unauthenticated(request.username(), request.password()));
        } catch (AuthenticationException exception) {
            loginAttemptService.recordFailure(request.username(), remoteAddress);
            throw exception;
        }

        loginAttemptService.recordSuccess(request.username(), remoteAddress);
        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(authentication);
        SecurityContextHolder.setContext(context);
        securityContextRepository.saveContext(context, httpRequest, httpResponse);
        return UserIdentityResponse.from(authentication);
    }

    @PostMapping("/logout")
    public ResponseEntity<Void> logout(HttpServletRequest request) {
        HttpSession session = request.getSession(false);
        if (session != null) {
            session.invalidate();
        }
        SecurityContextHolder.clearContext();
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/me")
    public UserIdentityResponse me(Authentication authentication) {
        return UserIdentityResponse.from(authentication);
    }
}
