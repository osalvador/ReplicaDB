package org.replicadb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class NoSpringBootOnClasspathTest {

    @Test
    void springBootIsAbsentFromCliClasspath() {
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("org.springframework.boot.SpringApplication"));
    }

    @Test
    void rootPomDoesNotDeclareSpringDependencies() throws IOException {
        String pom = Files.readString(Path.of("pom.xml"));

        assertFalse(pom.contains("<groupId>org.springframework"));
    }
}