package no.difi.statistics.authentication;

import no.difi.statistics.authentication.api.AuthenticationRequest;
import no.difi.statistics.authentication.api.AuthenticationResponse;
import no.difi.statistics.authentication.api.CredentialsResponse;
import no.difi.statistics.authentication.config.AppConfig;
import no.difi.statistics.test.utils.ElasticsearchHelper;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = AppConfig.class, initializers = AuthenticationServiceTest.Initializer.class)
@RunWith(SpringRunner.class)
public class AuthenticationServiceTest {

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            GenericContainer backend = new GenericContainer("elasticsearch:5.1.1");
            backend.start();
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.elasticsearch.host=" + backend.getContainerIpAddress(),
                    "no.difi.statistics.elasticsearch.port=" + backend.getMappedPort(9300)
            );
            AuthenticationServiceTest.backend = backend;
        }

    }

    private static GenericContainer backend;
    private ElasticsearchHelper elasticsearchHelper;

    @Autowired
    private Client client;
    @Autowired
    private TestRestTemplate restTemplate;

    @Before
    public void prepare() throws Exception {
        elasticsearchHelper = new ElasticsearchHelper(
                client,
                backend.getContainerIpAddress(),
                backend.getMappedPort(9200)
        );
        elasticsearchHelper.waitForGreenStatus();
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        elasticsearchHelper.clear();
    }

    @AfterClass
    public static void cleanupAll() {
        backend.stop();
    }

    @Test
    public void whenAuthenticatingWithValidCredentialsThenResponseIs201Created() {
        final String user = "aUser";
        final String password = "aPassword";
        permitUser(user, password);
        ResponseEntity<AuthenticationResponse> response = authenticate(requestEntity(user, password));
        assertAuthenticated(response);
    }

    @Test
    public void whenAuthenticatingWithInvalidCredentialsThenResponseIs204NoContent() {
        ResponseEntity<AuthenticationResponse> response = authenticate(requestEntity("aUser", "aPassword"));
        assertNotAuthenticated(response);
    }

    @Test
    public void whenCreatingCredentialsThenResponseIs201CreatedAndPassword() {
        final String username = "aUser";
        ResponseEntity<CredentialsResponse> response = createCredentials(username);
        assertEquals(201, response.getStatusCodeValue());
        assertNotNull(response.getBody().getPassword());
    }

    @Test
    public void whenCreatingCredentialsThenTheyCanBeUsedForAuthentication() {
        final String username = "aUser";
        ResponseEntity<CredentialsResponse> credentialsResponse = createCredentials(username);
        String password = credentialsResponse.getBody().getPassword();
        ResponseEntity<AuthenticationResponse> authenticationResponse = authenticate(requestEntity(username, password));
        assertAuthenticated(authenticationResponse);
    }

    private ResponseEntity<AuthenticationResponse> authenticate(HttpEntity<AuthenticationRequest> request) {
        return restTemplate.postForEntity("/authentications", request, AuthenticationResponse.class);
    }

    private ResponseEntity<CredentialsResponse> createCredentials(String username) {
        return restTemplate.postForEntity("/credentials/{username}", new HttpEntity<>("ABC"), CredentialsResponse.class, username);
    }


    private HttpEntity<AuthenticationRequest> requestEntity(String user, String password) {
        return new HttpEntity<>(AuthenticationRequest.builder().username(user).password(password).build());
    }

    private void assertAuthenticated(ResponseEntity<AuthenticationResponse> response) {
        assertEquals(201, response.getStatusCodeValue());
        assertTrue(response.getBody().isAuthenticated());
    }

    private void assertNotAuthenticated(ResponseEntity<AuthenticationResponse> response) {
        assertEquals(204, response.getStatusCodeValue());
        assertNull(response.getBody());
    }

    private void permitUser(String username, String password) {
        elasticsearchHelper.index("authentication", "authentication", username, document(username, password));
    }

    private Map<String, String> document(String username, String password) {
        Map<String, String> document = new HashMap<>();
        document.put("username", username);
        document.put("password", new BCryptPasswordEncoder().encode(password));
        return document;
    }

}
