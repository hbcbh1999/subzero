package com.subzero;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
public class SpringTest {
    
    @Autowired
    private WebTestClient webTestClient;

    @Test
    public void exampleTest() throws Exception {
        webTestClient.get().uri("/testquery")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("Windows 7");
    }

    @Test
    public void subzeroTest() throws Exception {
        webTestClient.get().uri("/rest/projects?select=name&id=eq.1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("[{\"name\":\"Windows 7\"}]");
    }
}
