package org.replicadb.server.audit;

import org.replicadb.server.audit.domain.AuditActor;
import org.replicadb.server.security.auth.ReplicaDbUserDetails;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Component
public class AuditActorResolver {

    public AuditActor resolve(Authentication authentication) {
        try {
            String sourceAddress = sourceAddress();
            if (authentication != null && authentication.getPrincipal() instanceof ReplicaDbUserDetails details
                    && details.userId() != null) {
                return new AuditActor(details.userId(), details.getUsername(), sourceAddress);
            }
            return anonymous(sourceAddress);
        } catch (RuntimeException exception) {
            return anonymous(null);
        }
    }

    public AuditActor forAttemptedLogin(String username, String sourceAddress) {
        String actorUsername = username == null || username.isBlank() ? "unknown" : username;
        return new AuditActor(null, actorUsername, sourceAddress);
    }

    public AuditActor system(String executorIdentity) {
        return AuditActor.system(executorIdentity == null ? "api" : executorIdentity);
    }

    private static String sourceAddress() {
        RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
        if (attributes instanceof ServletRequestAttributes servletRequestAttributes) {
            return servletRequestAttributes.getRequest().getRemoteAddr();
        }
        return null;
    }

    private static AuditActor anonymous(String sourceAddress) {
        return new AuditActor(null, "anonymous", sourceAddress);
    }
}
