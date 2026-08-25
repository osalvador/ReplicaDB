package org.replicadb.server.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.replicadb.server.config.PostgresTestcontainersConfig;
import org.replicadb.server.security.domain.AppUser;
import org.replicadb.server.security.domain.GlobalRole;
import org.replicadb.server.security.persistence.AppUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.ActiveProfiles;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("api")
@Import(PostgresTestcontainersConfig.class)
class RealHttpAuthenticationIT {

    @LocalServerPort
    private int port;

    @Autowired
    private AppUserRepository userRepository;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private PasswordEncoder passwordEncoder;

    private final HttpClient firstClient = HttpClient.newHttpClient();
    private final HttpClient secondClient = HttpClient.newHttpClient();

    @BeforeEach
    void clearState() {
        jdbcTemplate.update("TRUNCATE TABLE login_attempt, audit_event, SPRING_SESSION_ATTRIBUTES, SPRING_SESSION, app_user CASCADE",
                Map.of());
    }

    @Test
    void failedLoginsFromSeparateClientsShareTheDatabaseThrottle() throws Exception {
        String username = "http-user-" + UUID.randomUUID();
        userRepository.insert(new AppUser(null, username, passwordEncoder.encode("correct-password"),
                GlobalRole.VIEWER, true, null, null));

        for (int attempt = 0; attempt < 5; attempt++) {
            HttpClient client = attempt % 2 == 0 ? firstClient : secondClient;
            HttpResponse<String> response = login(client, username, "wrong-password");
            assertEquals(401, response.statusCode());
        }

        HttpResponse<String> blocked = login(secondClient, username, "correct-password");
        assertEquals(429, blocked.statusCode());
    }

    private HttpResponse<String> login(HttpClient client, String username, String password)
            throws Exception {
        String body = "{\"username\":\"" + username + "\",\"password\":\"" + password + "\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/auth/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
