package org.replicadb.server.security.execution;

import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.replicadb.server.audit.AuditActorResolver;
import org.replicadb.server.audit.AuditService;
import org.replicadb.server.audit.domain.AuditAction;
import org.replicadb.server.audit.domain.AuditOutcome;
import org.replicadb.server.audit.domain.AuditResourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Map;
import java.util.function.Function;

@Component
@ConditionalOnProperty(name = "replicadb.security.bootstrap.enabled", havingValue = "true", matchIfMissing = true)
public class AdminBootstrapRunner implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminBootstrapRunner.class);
    private static final String USERNAME_VARIABLE = "REPLICADB_BOOTSTRAP_ADMIN_USERNAME";
    private static final String PASSWORD_VARIABLE = "REPLICADB_BOOTSTRAP_ADMIN_PASSWORD";

    private final AppUserRepository appUserRepository;
    private final PasswordEncoder passwordEncoder;
    private final Function<String, String> envLookup;
    private final AuditService auditService;
    private final AuditActorResolver auditActorResolver;

    @Autowired
    public AdminBootstrapRunner(AppUserRepository appUserRepository,
                                PasswordEncoder passwordEncoder,
                                Environment environment,
                                AuditService auditService,
                                AuditActorResolver auditActorResolver) {
        this(appUserRepository, passwordEncoder,
                name -> firstNonNull(System.getenv(name), environment.getProperty(name)),
                auditService, auditActorResolver);
    }

    AdminBootstrapRunner(AppUserRepository appUserRepository,
                         PasswordEncoder passwordEncoder,
                         Function<String, String> envLookup,
                         AuditService auditService,
                         AuditActorResolver auditActorResolver) {
        this.appUserRepository = Objects.requireNonNull(appUserRepository);
        this.passwordEncoder = Objects.requireNonNull(passwordEncoder);
        this.envLookup = Objects.requireNonNull(envLookup);
        this.auditService = Objects.requireNonNull(auditService);
        this.auditActorResolver = Objects.requireNonNull(auditActorResolver);
    }

    @Override
    public void run(ApplicationArguments args) {
        if (appUserRepository.countByRole(GlobalRole.ADMIN) > 0) {
            return;
        }

        String username = envLookup.apply(USERNAME_VARIABLE);
        String password = envLookup.apply(PASSWORD_VARIABLE);
        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            throw new IllegalStateException("Bootstrap requires " + USERNAME_VARIABLE
                    + " and " + PASSWORD_VARIABLE + " when no ADMIN user exists");
        }

        AppUser bootstrapUser = new AppUser(
                null, username, passwordEncoder.encode(password), GlobalRole.ADMIN, true, null, null);
        try {
            AppUser persisted = appUserRepository.insert(bootstrapUser);
            auditService.record(auditActorResolver.system("bootstrap"), AuditAction.USER_CREATED,
                AuditResourceType.USER, persisted.id().toString(), AuditOutcome.SUCCESS,
                Map.of("username", persisted.username(), "role", persisted.role().name()));
            LOGGER.warn("Created bootstrap ADMIN user '{}'; rotate its password after first login", username);
        } catch (DuplicateKeyException exception) {
            if (appUserRepository.countByRole(GlobalRole.ADMIN) == 0) {
                throw exception;
            }
            LOGGER.info("Another instance completed ADMIN bootstrap; continuing");
        }
    }

    private static String firstNonNull(String first, String second) {
        return first == null ? second : first;
    }
}
