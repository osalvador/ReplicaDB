package org.replicadb.server.audit;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.core.Authentication;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AuditActorResolverTest {

    private final AuditActorResolver resolver = new AuditActorResolver();

    @AfterEach
    void clearRequestContext() {
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    void resolvesUserIdAndUsernameFromReplicaDbPrincipal() {
        UUID userId = UUID.randomUUID();
        ReplicaDbUserDetails details = new ReplicaDbUserDetails(new AppUser(userId, "admin", "hash",
                GlobalRole.ADMIN, true, null, null));
        Authentication authentication = authenticationWithPrincipal(details);

        AuditActor actor = resolver.resolve(authentication);

        assertEquals(userId, actor.userId());
        assertEquals("admin", actor.username());
    }

    @Test
    void returnsAnonymousForNullAuthentication() {
        AuditActor actor = resolver.resolve(null);

        assertNull(actor.userId());
        assertEquals("anonymous", actor.username());
    }

    @Test
    void returnsAnonymousForUnexpectedPrincipal() {
        AuditActor actor = resolver.resolve(authenticationWithPrincipal("plain-principal"));

        assertNull(actor.userId());
        assertEquals("anonymous", actor.username());
    }

    @Test
    void leavesSourceAddressNullWithoutRequestContext() {
        AuditActor actor = resolver.resolve(null);

        assertNull(actor.sourceAddress());
    }

    @Test
    void readsSourceAddressFromBoundRequest() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("10.0.0.1");
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));

        AuditActor actor = resolver.resolve(null);

        assertEquals("10.0.0.1", actor.sourceAddress());
    }

    @Test
    void usesUnknownForMissingAttemptedUsername() {
        AuditActor actor = resolver.forAttemptedLogin(null, "10.0.0.1");

        assertEquals("unknown", actor.username());
        assertEquals("10.0.0.1", actor.sourceAddress());
    }

    @Test
    void createsSystemActorsWithApiFallback() {
        assertEquals("system:scheduler", resolver.system("scheduler").username());
        assertEquals("system:api", resolver.system(null).username());
    }

    private static Authentication authenticationWithPrincipal(Object principal) {
        Authentication authentication = org.mockito.Mockito.mock(Authentication.class);
        org.mockito.Mockito.when(authentication.getPrincipal()).thenReturn(principal);
        return authentication;
    }
}
