package org.replicadb.server.security.execution;

import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.Objects;
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

    @Autowired
    public AdminBootstrapRunner(AppUserRepository appUserRepository,
                                PasswordEncoder passwordEncoder,
                                Environment environment) {
        this(appUserRepository, passwordEncoder,
                name -> firstNonNull(System.getenv(name), environment.getProperty(name)));
    }

    AdminBootstrapRunner(AppUserRepository appUserRepository,
                         PasswordEncoder passwordEncoder,
                         Function<String, String> envLookup) {
        this.appUserRepository = Objects.requireNonNull(appUserRepository);
        this.passwordEncoder = Objects.requireNonNull(passwordEncoder);
        this.envLookup = Objects.requireNonNull(envLookup);
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
            appUserRepository.insert(bootstrapUser);
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
