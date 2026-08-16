package org.replicadb.server.job.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Set;
import java.util.NoSuchElementException;

import jakarta.validation.ConstraintViolationException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class GlobalExceptionHandlerTest {

    private MockMvc mockMvc;
    private GlobalExceptionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new GlobalExceptionHandler();
        mockMvc = MockMvcBuilders.standaloneSetup(new ThrowingController())
                .setControllerAdvice(handler)
                .build();
    }

    @Test
    void mapsIllegalArgumentToBadRequestProblem() throws Exception {
        assertProblem("/illegal", 400);
    }

    @Test
    void mapsNotFoundToNotFoundProblem() throws Exception {
        assertProblem("/missing", 404);
    }

    @Test
    void mapsIllegalStateToConflictProblem() throws Exception {
        assertProblem("/state", 409);
    }

    @Test
    void mapsUnexpectedExceptionsToInternalServerErrorProblem() throws Exception {
        var result = mockMvc.perform(get("/unexpected"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andReturn();

        assertFalse(result.getResponse().getContentAsString().contains("secret"));
    }

    @Test
    void mapsInvalidRequestBodyToBadRequestProblem() throws Exception {
        assertProblem(post("/validation").contentType(MediaType.APPLICATION_JSON).content("{}"), 400);
    }

    @Test
    void mapsConstraintViolationsToBadRequestProblem() {
        var problem = handler.handleConstraintViolation(new ConstraintViolationException(Set.of()));

        assertEquals(400, problem.getStatus());
    }

    private void assertProblem(String path, int status) throws Exception {
        assertProblem(get(path), status);
    }

    private void assertProblem(org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder request,
                               int status) throws Exception {
        mockMvc.perform(request)
                .andExpect(status().is(status))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(status))
                .andExpect(jsonPath("$.detail").isNotEmpty());
    }

    @RestController
    static class ThrowingController {

        @GetMapping("/illegal")
        void illegal() {
            throw new IllegalArgumentException("invalid request");
        }

        @GetMapping("/missing")
        void missing() {
            throw new NoSuchElementException("missing resource");
        }

        @GetMapping("/state")
        void state() {
            throw new IllegalStateException("conflicting state");
        }

        @GetMapping("/unexpected")
        void unexpected() {
            throw new RuntimeException("jdbc:source;password=secret");
        }

        @PostMapping("/validation")
        void validation(@Valid @RequestBody ValidRequest request) {
        }
    }

    record ValidRequest(@NotBlank String value) {
    }
}
