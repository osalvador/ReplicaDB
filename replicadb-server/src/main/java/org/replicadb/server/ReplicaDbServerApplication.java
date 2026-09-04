package org.replicadb.server;

import org.replicadb.server.local.EmbeddedPostgresLaunchOptions;
import org.replicadb.server.local.EmbeddedPostgresRuntime;
import org.replicadb.server.local.EmbeddedPostgresRuntimeFactory;
import org.replicadb.server.local.EmbeddedPostgresShutdownLifecycle;
import org.replicadb.server.local.LocalMasterKeyBootstrap;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

@SpringBootApplication
public class ReplicaDbServerApplication {

    public static void main(String[] args) {
        launch(args, System.getProperties(), System.getenv());
    }

    public static ConfigurableApplicationContext launch(String[] args, java.util.Properties systemProperties,
                                                        java.util.Map<String, String> environment) {
        EmbeddedPostgresLaunchOptions launchOptions = EmbeddedPostgresLaunchOptions.resolve(
                args, systemProperties, environment);
        if (!launchOptions.isEmbeddedPostgresEnabled()) {
            return SpringApplication.run(ReplicaDbServerApplication.class, args);
        }

        return launchWithEmbeddedPostgres(launchOptions, environment);
    }

    private static ConfigurableApplicationContext launchWithEmbeddedPostgres(
            EmbeddedPostgresLaunchOptions launchOptions, java.util.Map<String, String> environment) {
        Path keyringPath;
        EmbeddedPostgresRuntime runtime;
        try {
            keyringPath = new LocalMasterKeyBootstrap().prepare(
                    launchOptions.getEmbeddedPostgresProperties().getHome(),
                    launchOptions.getResolvedProperties(), environment);
            runtime = new EmbeddedPostgresRuntimeFactory().start(
                    launchOptions.getEmbeddedPostgresProperties());
        } catch (IOException exception) {
            throw new IllegalStateException("Could not start embedded PostgreSQL", exception);
        }

        try {
            SpringApplication application = new SpringApplication(ReplicaDbServerApplication.class);
            application.setAdditionalProfiles("api");
            application.setDefaultProperties(launchOptions.getSpringDefaults(runtime, keyringPath, environment));
                application.addInitializers(context -> context.getBeanFactory().registerSingleton(
                    "embeddedPostgresShutdownLifecycle", new EmbeddedPostgresShutdownLifecycle(runtime)));
            return application.run(embeddedArguments(launchOptions.getArguments()));
        } catch (RuntimeException | Error exception) {
            closeRuntimeAfterFailure(runtime, exception);
            throw exception;
        }
    }

    private static void closeRuntimeAfterFailure(EmbeddedPostgresRuntime runtime, Throwable failure) {
        try {
            runtime.close();
        } catch (IOException exception) {
            failure.addSuppressed(exception);
        }
    }

    private static String[] embeddedArguments(String[] arguments) {
        String[] internalOverrides = {
            "--replicadb.security.bootstrap.enabled=true",
            "--server.servlet.session.cookie.secure=false"
        };
        String[] combined = Arrays.copyOf(arguments, arguments.length + internalOverrides.length);
        System.arraycopy(internalOverrides, 0, combined, arguments.length, internalOverrides.length);
        return combined;
    }
}
