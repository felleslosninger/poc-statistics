package no.difi.statistics.ingest.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.model.TimeSeriesPoint;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.apache.tomcat.util.codec.binary.Base64.encodeBase64;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {AppConfig.class, MockBackendConfig.class})
@AutoConfigureMockMvc
public class IngestRestControllerTest {

    @Autowired
    private IngestService service;
    @Autowired
    private UserDetailsService userDetailsService;

    @Autowired
    private MockMvc mockMvc;

    @After
    public void resetMocks() {
        reset(service);
        reset(userDetailsService);
    }

    @Test
    public void whenRequestingIndexThenAuthorizationIsNotRequired() throws Exception {
        mockMvc.perform(get("/")).andExpect(status().is(200));
    }

    @Test
    public void whenIngestingAndUserIsNotTheSameAsOwnerThenAccessIsDenied() throws Exception {
        permitUser("aUser", "aPassword");
        mockMvc.perform(
                request()
                        .owner("anotherUser")
                        .series("aTimeSeries")
                        .user("aUser")
                        .password("aPassword")
                        .content(json(aPoint()))
                        .build()
        )
                .andExpect(status().is(403));
    }

    @Test
    public void whenSendingRequestWithValidTimeSeriesPointAndValidLoginThenExpectValuesSentToServiceMethodToBeTheSameAsSentToService() throws Exception {
        permitUser("aUser", "aPassword");
        TimeSeriesPoint timeSeriesPoint = aPoint();
        mockMvc.perform(
                request()
                        .owner("aUser")
                        .series("aTimeSeries")
                        .user("aUser")
                        .password("aPassword")
                        .content(json(timeSeriesPoint))
                        .build()
        )
                .andExpect(status().is(200));
        verify(service).minute(eq("aTimeSeries"), eq("aUser"), eq(timeSeriesPoint));
    }

    @Test
    public void whenSendingValidRequestThenExpectNormalResponse() throws Exception {
        permitUser("aUser", "aPassword");
        mockMvc.perform(
                request()
                        .owner("aUser")
                        .series("aTimeSeries")
                        .user("aUser")
                        .password("aPassword")
                        .content(json(aPoint()))
                        .build()
        )
                .andExpect(status().is(200));
    }

    @Test
    public void whenSendingRequestWithInvalidContentThenExpect400Response() throws Exception {
        permitUser("aUser", "aPassword");
        mockMvc.perform(
                request()
                        .owner("aUser")
                        .series("aTimeSeries")
                        .user("aUser")
                        .password("aPassword")
                        .content("invalidJson")
                        .build()
        )
                .andExpect(status().is(400));
    }

    @Test
    public void whenSendingRequestWithWrongPasswordThenExpect401Response() throws Exception {
        permitUser("aUser", "aPassword");
        mockMvc.perform(
                request()
                        .owner("aUser")
                        .series("aTimeSeries")
                        .user("aUser")
                        .password("wrongPassword")
                        .content(json(aPoint()))
                        .build()
        )
                .andExpect(status().is(401));
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
        private String owner;
        private String series;
        private String user;
        private String password;
        private String content;

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

    private void permitUser(String user, String password) {
        when(userDetailsService.loadUserByUsername(user)).thenReturn(userDetails(user, password));
    }

    private UserDetails userDetails(String user, String password) {
        return new User(user, password, singletonList(new SimpleGrantedAuthority("USER")));
    }

}
