package org.replicadb.server;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ServerPortPlaceholderTest {

    @Test
    void apiUsesPlatformPortWhenProvided() {
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("spring.config.name=application", "spring.profiles.active=api", "PORT=9500")
                .run(context -> assertEquals(9500, context.getEnvironment().getProperty("server.port", Integer.class)));
    }

    @Test
    void apiUsesDefaultPortWhenPlatformPortIsAbsent() {
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("spring.config.name=application", "spring.profiles.active=api")
                .run(context -> assertEquals(8080, context.getEnvironment().getProperty("server.port", Integer.class)));
    }

    @Test
    void workerRemainsHttpFreeWhenPlatformPortIsProvided() {
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withPropertyValues("spring.config.name=application", "spring.profiles.active=worker", "PORT=9500")
                .run(context -> assertEquals(-1, context.getEnvironment().getProperty("server.port", Integer.class)));
    }
}