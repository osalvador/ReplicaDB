package org.replicadb.server.security;

import org.replicadb.server.security.domain.GlobalRole;
import org.springframework.security.test.context.support.WithSecurityContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@WithSecurityContext(factory = WithMockReplicaDbUserSecurityContextFactory.class)
public @interface WithMockReplicaDbUser {

    String userId() default "00000000-0000-0000-0000-000000000001";

    String username() default "test-user";

    GlobalRole role() default GlobalRole.OPERATOR;
}
