package org.replicadb.server.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class SpaFallbackController {

    @GetMapping({
            "/",
            "/login",
            "/profile",
            "/jobs",
            "/jobs/{*path}",
            "/datasources",
            "/datasources/{*path}",
            "/runs/{*path}",
            "/audit",
            "/users"
    })
    String forwardToSpa() {
        return "forward:index.html";
    }
}
