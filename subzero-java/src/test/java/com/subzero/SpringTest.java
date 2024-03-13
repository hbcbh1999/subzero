package com.subzero;

import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.assertThat;

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
    
    @Test
    public void permissionsTest() throws Exception {
        String expectedJsonString = "[{\"id\":1,\"value\":\"One Alice Public\",\"hidden\":\"Hidden\",\"role\":\"alice\"},{\"id\":2,\"value\":\"Two Bob Public\",\"hidden\":\"Hidden\",\"role\":\"bob\"},{\"id\":3,\"value\":\"Three Charlie Public\",\"hidden\":\"Hidden\",\"role\":\"charlie\"},{\"id\":10,\"value\":\"Ten Alice Private\",\"hidden\":\"Hidden\",\"role\":\"alice\"},{\"id\":11,\"value\":\"Eleven Alice Private\",\"hidden\":\"Hidden\",\"role\":\"alice\"}]";
        ObjectMapper objectMapper = new ObjectMapper();
        webTestClient.get().uri("/rest/permissions_check?select=id,value,hidden,role")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(actualJsonString -> {
                    try {
                        JsonNode expectedJsonNode = objectMapper.readTree(expectedJsonString);
                        JsonNode actualJsonNode = objectMapper.readTree(actualJsonString);
                        assertThat(actualJsonNode).isEqualTo(expectedJsonNode);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
