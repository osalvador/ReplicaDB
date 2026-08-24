package org.replicadb.server.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@Profile("api")
@EnableScheduling
public class ApiSchedulingConfiguration {
}