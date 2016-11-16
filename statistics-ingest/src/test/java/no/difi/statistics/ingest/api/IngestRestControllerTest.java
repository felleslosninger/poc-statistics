package no.difi.statistics.ingest.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.model.TimeSeriesPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.web.client.RestTemplate;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.apache.tomcat.util.codec.binary.Base64.encodeBase64;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = {AppConfig.class, MockBackendConfig.class})
@AutoConfigureMockMvc
public class IngestRestControllerTest {
    public static final String URI_MINUTE = "/{owner}/{seriesName}/minute";
    public static final String URI_HOUR = "/{owner}/{seriesName}/hour";

    @Autowired
    private RestTemplate authenticationRestTemplate;
    private MockRestServiceServer authenticationService;

    @Before
    public void setup() {
        authenticationService = MockRestServiceServer.bindTo(authenticationRestTemplate).build();
    }

    @Autowired
    private RestTemplate authenticationRestTemplate;
    private MockRestServiceServer authenticationService;

    @Before
    public void setup() {
        authenticationService = MockRestServiceServer.bindTo(authenticationRestTemplate).build();
    }

    @Autowired
    private IngestService service;

    @Autowired
    private MockMvc mockMvc;

    @After
    public void resetMocks() {
        reset(service);
    }

    @Test
    public void whenRequestingIndexThenAuthorizationIsNotRequired() throws Exception {
        mockMvc.perform(get("/")).andExpect(status().is(302)).andExpect(header().string("Location", equalTo("swagger-ui.html")));
        mockMvc.perform(get("/swagger-ui.html")).andExpect(status().is(200));
    }

    @Test
    public void whenIngestingAndUserIsNotTheSameAsOwnerThenAccessIsDenied() throws Exception {
        validCredentials("aUser", "aPassword");
        mockMvc.perform(
                request()
                        .owner("anotherUser")
                        .user("aUser")
                        .content(json(aPoint()))
                        .build()
        )
                .andExpect(status().is(403));
    }

    @Test
    public void whenSendingRequestWithValidTimeSeriesPointAndValidLoginThenExpectValuesSentToServiceMethodToBeTheSameAsSentToService() throws Exception {
        validCredentials("aUser", "aPassword");
        TimeSeriesPoint timeSeriesPoint = aPoint();
        mockMvc.perform(
                request()
                        .series("aTimeSeries")
                        .content(json(timeSeriesPoint))
                        .build()
        )
                .andExpect(status().is(200));
        verify(service).minute(eq("aTimeSeries"), eq("aUser"), eq(timeSeriesPoint));
    }

    @Test
    public void whenSendingValidMinuteRequestThenExpectNormalResponse() throws Exception {
        validCredentials("aUser", "aPassword");
        mockMvc.perform(
                request()
                        .content(json(aPoint()))
                        .uri(URI_MINUTE)
                        .build()
        )
                .andExpect(status().is(200));
    }

    @Test
    public void whenSendingRequestWithInvalidContentThenExpect400Response() throws Exception {
        validCredentials("aUser", "aPassword");
        mockMvc.perform(
                request()
                        .content("invalidJson")
                        .build()
        )
                .andExpect(status().is(400));
    }

    @Test
    public void whenSendingRequestWithWrongPasswordThenExpect401Response() throws Exception {
        invalidCredentials("aUser", "wrongPassword");
        mockMvc.perform(
                request()
                        .owner("aUser")
                        .user("aUser")
                        .password("wrongPassword")
                        .content(json(aPoint()))
                        .build()
        )
                .andExpect(status().is(401));
    }

    @Test
    public void whenSendingValidHourRequestThenExpectNormalResponse() throws Exception {
        validCredentials("aUser", "aPassword");
        mockMvc.perform(
                request()
                        .content(json(aPoint()))
                        .uri(URI_HOUR)
                        .build()
        )
                .andExpect(status().is(HttpStatus.OK.value()));
    }

    private TimeSeriesPoint aPoint() {
        return TimeSeriesPoint.builder()
                .measurement("antall", 2)
                .timestamp(ZonedDateTime.of(2016, 3, 3, 20, 12, 13, 12, ZoneId.of("UTC")))
                .build();
    }

    public static RequestBuilder request() {
        return new RequestBuilder();
    }

    public static class RequestBuilder {
        private String owner = "aUser";
        private String series = "aTimeSeries";
        private String user = "aUser";
        private String password = "aPassword";
        private String content;
        private String uri = URI_MINUTE;

        RequestBuilder owner(String owner) {
            this.owner = owner;
            return this;
        }

        RequestBuilder series(String series) {
            this.series = series;
            return this;
        }

        RequestBuilder user(String user) {
            this.user = user;
            return this;
        }

        RequestBuilder password(String password) {
            this.password = password;
            return this;
        }

        RequestBuilder content(String content) {
            this.content = content;
            return this;
        }

        private String authorizationHeader(String username, String password) {
            return "Basic " + new String(encodeBase64((username + ":" + password).getBytes()));
        }

        MockHttpServletRequestBuilder build() {
            return post("/{owner}/{seriesName}/minute", owner, series)
                    .contentType(MediaType.APPLICATION_JSON_UTF8)
                    .header("Authorization", authorizationHeader(user, password))
                    .content(content);
        }

    }

    private String json(TimeSeriesPoint timeSeriesPoint) throws Exception {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .writeValueAsString(timeSeriesPoint);
    }

    private void validCredentials(String username, String password) {
        authenticationService
                .expect(once(), requestTo("http://authenticate:8080/authentications"))
                .andExpect(method(POST))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("username", equalTo(username)))
                .andExpect(jsonPath("password", equalTo(password)))
                .andRespond(withSuccess("{\"authenticated\": true}", MediaType.APPLICATION_JSON_UTF8));
    }

    private void invalidCredentials(String username, String password) {
        authenticationService
                .expect(once(), requestTo("http://authenticate:8080/authentications"))
                .andExpect(method(POST))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("username", equalTo(username)))
                .andExpect(jsonPath("password", equalTo(password)))
                .andRespond(withSuccess("{\"authenticated\": false}", MediaType.APPLICATION_JSON_UTF8));
    }

}
