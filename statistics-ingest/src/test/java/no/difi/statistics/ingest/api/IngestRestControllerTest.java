package no.difi.statistics.ingest.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static no.difi.statistics.model.MeasurementDistance.minutes;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpHeaders.AUTHORIZATION;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = {AppConfig.class, MockBackendConfig.class})
@AutoConfigureMockMvc
public class IngestRestControllerTest {

    static final String PREFIX = "digdir";
    static final String SUBSCOPE = "statistikk.skriv";
    final String SCOPE = PREFIX + ":"+SUBSCOPE;
    static final String OWNER = "991825827";


    @Autowired
    private IngestService service;

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    public JwtDecoder jwtDecoder;

    @After
    public void resetMocks() {
        reset(jwtDecoder);
    }

    @Test
    public void whenRequestingIndexThenAuthorizationIsNotRequired() throws Exception {
        mockMvc.perform(get("/")).andExpect(status().is(HttpStatus.FOUND.value())).andExpect(header().string("Location", equalTo("swagger-ui.html")));
        mockMvc.perform(get("/swagger-ui.html")).andExpect(status().is(HttpStatus.OK.value()));
    }

    @Test
    public void whenIngestingAndUserIsNotTheSameAsOwnerThenAccessIsDenied() throws Exception {


        when(jwtDecoder.decode(anyString())).thenReturn(mockJwt(OWNER, SCOPE));
        mockMvc.perform(
                request()
                        .owner("anotherOrgNo")
                        .content(json(singletonList(aPoint())))
                        .distance("minutes")
                        .ingest()
        )
                .andExpect(status().is(HttpStatus.FORBIDDEN.value()));
    }


    @Test
    public void whenSendingRequestWithValidTimeSeriesPointAndValidLoginThenExpectValuesSentToServiceMethodToBeTheSameAsSentToService() throws Exception {
        final String orgno = "984936923";
        when(jwtDecoder.decode(anyString())).thenReturn(mockJwt(orgno, SCOPE));
        TimeSeriesPoint timeSeriesPoint = aPoint();
        mockMvc.perform(
                request()
                        .content(json(singletonList(timeSeriesPoint)))
                        .owner(orgno)
                        .distance("minutes")
                        .ingest()
        )
                .andExpect(status().is(HttpStatus.OK.value()));
        verify(service).ingest(
                eq(TimeSeriesDefinition.builder().name("aTimeSeries").distance(minutes).owner(orgno)),
                eq(singletonList(timeSeriesPoint))
        );
    }

    @Test
    public void whenSendingValidMinuteRequestThenExpectNormalResponse() throws Exception {
        when(jwtDecoder.decode(anyString())).thenReturn(mockJwt(OWNER, SCOPE));
        mockMvc.perform(
                request()
                        .content(json(singletonList(aPoint())))
                        .distance("minutes")
                        .ingest()
        )
                .andExpect(status().is(HttpStatus.OK.value()));
    }

    @Test
    public void whenSendingRequestWithInvalidContentThenExpect400Response() throws Exception {
        when(jwtDecoder.decode(anyString())).thenReturn(mockJwt(OWNER, SCOPE));
        mockMvc.perform(
                request()
                        .content("invalidJson")
                        .distance("minutes")
                        .ingest()
        )
                .andExpect(status().is(HttpStatus.BAD_REQUEST.value()));
    }

    @Test
    public void whenSendingRequestWithWrongPasswordThenExpect403Response() throws Exception {
        final String notOwnerOrgNo = "975700844";
        when(jwtDecoder.decode(anyString())).thenReturn(mockJwt(notOwnerOrgNo, SCOPE));
        mockMvc.perform(
                request().owner(OWNER)
                        .content(json(singletonList(aPoint())))
                        .distance("minutes")
                        .ingest()
        )
                .andExpect(status().is(HttpStatus.FORBIDDEN.value()));
    }

    @Test
    public void whenRequestingLastPointInASeriesThenNoAuthenticationIsRequired() throws Exception {
        when(service.last(any(TimeSeriesDefinition.class))).thenReturn(aPoint());
        mockMvc.perform(request().distance("minutes").last())
                .andExpect(status().is(HttpStatus.OK.value()));
    }

    @Test
    public void whenRequestingLastPointInEmptySeriesThenExpectEmptyResponse() throws Exception {
        mockMvc.perform(request().distance("minutes").last())
                .andExpect(status().is(HttpStatus.NO_CONTENT.value()))
                .andExpect(MockMvcResultMatchers.content().string(""));
    }

    @Test
    public void whenSendingValidHourRequestThenExpectNormalResponse() throws Exception {
        when(jwtDecoder.decode(anyString())).thenReturn(mockJwt(OWNER, SCOPE));
        mockMvc.perform(request().content(json(singletonList(aPoint()))).distance("hours").ingest())
                .andExpect(status().is(HttpStatus.OK.value()));
    }

    @Test
    public void whenBulkIngestingTwoPointThenExpectOkResponse() throws Exception {
        when(jwtDecoder.decode(anyString())).thenReturn(mockJwt(OWNER, SCOPE));
        mockMvc.perform(request().content(json(asList(aPoint(), aPoint()))).distance("hours").ingest())
                .andExpect(status().is(HttpStatus.OK.value()));
    }

    @Test
    public void whenAccessTokenHasInvalidScopeThenExpect403() throws Exception{
        when(jwtDecoder.decode(anyString())).thenReturn(mockJwt(OWNER, "invalidScope"));
        mockMvc.perform(request().content(json(singletonList(aPoint()))).distance("hours").ingest())
                .andExpect(status().is(HttpStatus.FORBIDDEN.value()));
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
        private String owner = OWNER;
        private String series = "aTimeSeries";
        private String content;
        private String distance;

        RequestBuilder owner(String owner) {
            this.owner = owner;
            return this;
        }

        RequestBuilder series(String series) {
            this.series = series;
            return this;
        }


        RequestBuilder content(String content) {
            this.content = content;
            return this;
        }

        RequestBuilder distance(String distance) {
            this.distance = distance;
            return this;
        }


        MockHttpServletRequestBuilder ingest() {
            return post("/{owner}/{seriesName}/{distance}", owner, series, distance)
                    .contentType(MediaType.APPLICATION_JSON_UTF8)
                    .header(AUTHORIZATION, "Bearer token")
                    .content(content);
        }

        MockHttpServletRequestBuilder last() {
            return get("/{owner}/{seriesName}/{distance}/last", owner, series, distance);
        }

    }

    private String json(Object object) throws Exception {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .writeValueAsString(object);
    }

    // orgno digdir: 991825827
    private Jwt mockJwt(String orgno, String scope) {
        Map<String, Object> claims = new HashMap<>();
        claims.put("scope", scope);
        claims.put("consumer", orgno);
        final HashMap<String, String> consumer = new HashMap<>();
        consumer.put("authority","iso6523-actorid-upis");
        consumer.put("ID", "0192:"+orgno);
        claims.put("consumer", consumer);
        Map<String, Object> headers = new HashMap<>();
        headers.put("testheader", "test");
        return Jwt.withTokenValue("val").header("testheader", "testheader").claim("scope", scope).claim("consumer", consumer).issuedAt(Instant.now()).expiresAt(Instant.now().plusSeconds(50L)).build();
    }



}
