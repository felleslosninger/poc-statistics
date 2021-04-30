package no.difi.statistics.ingest.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.difi.statistics.ingest.client.model.IngestResponse;
import no.difi.statistics.ingest.client.model.TimeSeriesDefinition;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.lang.String.format;
import static java.time.ZonedDateTime.now;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static no.difi.statistics.ingest.client.model.IngestResponse.Status.Ok;
import static no.difi.statistics.ingest.client.model.MeasurementDistance.hours;
import static no.difi.statistics.ingest.client.model.MeasurementDistance.minutes;
import static no.difi.statistics.ingest.client.model.TimeSeriesDefinition.timeSeriesDefinition;
import static no.difi.statistics.ingest.client.model.TimeSeriesPoint.timeSeriesPoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IngestClientTest {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .registerModule(new Jdk8Module())
            .setDateFormat(new ISO8601DateFormat())
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    private static final String JSON = "application/json";
    private static final String content_type = "Content-Type";
    private static final String hostname = "localhost";

    private static final String series_name = "seriesname";

    private static final String owner = "999888777";
    private static final String BEARER_TOKEN = "fakeToken";
    private static final String valid_url = "/999888777/seriesname/minutes";
    private final ZonedDateTime aTimestamp = ZonedDateTime.of(2016, 3, 3, 0, 0, 0, 0, ZoneId.of("UTC"));

    private static IngestClient ingestClient;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig()
            .bindAddress(hostname)
            .dynamicPort());
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void before() throws MalformedURLException {
        ingestClient = new IngestClient(new URL("http://localhost:" + wireMockRule.port()), 500, 500, owner);
    }

    @Test
    public void shouldSucceedWhenValidRequest() {
        givenOkResponse(1);
        IngestResponse response = ingestClient.ingest(
                timeSeriesDefinition().name("aSeries").distance(hours),
                singletonList(aPoint()),BEARER_TOKEN
        );
        assertEquals(1, response.getStatuses().size());
        assertEquals(Ok, response.getStatuses().get(0));
    }

    @Test
    public void shouldFailWithFailedWhenSomethingFailsInTransmission() {
        createWiremockStub(HttpURLConnection.HTTP_OK);
        wireMockRule.addRequestProcessingDelay(10000);
        expectedEx.expect(IngestService.Failed.class);
        ingestClient.ingest(aSeriesDefinition(), twoPoints(), BEARER_TOKEN);
    }

    @Test
    public void shouldFailWithUnauthorizedExceptionWhenUnauthorized() {
        createWiremockStub(HttpURLConnection.HTTP_UNAUTHORIZED);

        expectedEx.expect(IngestClient.Unauthorized.class);
        expectedEx.expectMessage("Failed to authorize Ingest service");

        ingestClient.ingest(aSeriesDefinition(), twoPoints(), BEARER_TOKEN);
    }

    @Test
    public void shouldFailWithUnauthorizedExceptionWhenForbidden() {
        createWiremockStub(HttpURLConnection.HTTP_FORBIDDEN);

        expectedEx.expect(IngestClient.Unauthorized.class);
        expectedEx.expectMessage("Failed to authorize Ingest service");

        ingestClient.ingest(aSeriesDefinition(), twoPoints(), BEARER_TOKEN);
    }

    @Test
    public void shouldFailWithIngestFailExceptionWhenNotFound() {
        createWiremockStub(HttpURLConnection.HTTP_NOT_FOUND);

        expectedEx.expect(IngestService.Failed.class);
        ingestClient.ingest(aSeriesDefinition(), twoPoints(), BEARER_TOKEN);
    }

    @Test
    public void shouldSucceedWhenValidRequestWithAuthorizationForMinute() {
        givenOkResponse(1);
        ingestClient.ingest(timeSeriesDefinition().name(series_name).distance(minutes), singletonList(aPoint()), BEARER_TOKEN);
        verify(postRequestedFor(urlEqualTo(valid_url))
                .withHeader(content_type, equalTo(JSON)));
    }

    @Test
    public void shouldSucceedWhenValidRequestWithAuthorizationForHour() {
        givenOkResponse(1);
        ingestClient.ingest(timeSeriesDefinition().name(series_name).distance(minutes), singletonList(aPoint()), BEARER_TOKEN);
        verify(postRequestedFor(urlEqualTo(valid_url))
                .withHeader(content_type, equalTo(JSON)));
    }

    @Test
    public void shouldThrowConnectFailedWhenConnectionFails(){
        wireMockRule.stop();
        expectedEx.expect(IngestService.ConnectFailed.class);
        ingestClient.ingest(timeSeriesDefinition().name(series_name).distance(minutes), singletonList(aPoint()), BEARER_TOKEN);
    }

    @Test
    public void shouldThrowExceptionWhenContentTypeIsWrong() {
        createStub(HttpURLConnection.HTTP_UNSUPPORTED_TYPE);

        expectedEx.expect(IngestService.Failed.class);
        expectedEx.expectMessage("Ingest failed (415)");

        ingestClient.ingest(timeSeriesDefinition().name(series_name).distance(minutes), singletonList(aPoint()), BEARER_TOKEN);
    }

    @Test
    public void shouldGetAuthenticationErrorWhenAuthenticationFails() {
        createStub(HttpURLConnection.HTTP_UNAUTHORIZED);
        expectedEx.expect(IngestService.Unauthorized.class);
        ingestClient.ingest(timeSeriesDefinition().name(series_name).distance(minutes), singletonList(aPoint()), BEARER_TOKEN);
    }

    @Test
    public void shouldReturnLastWhenLastRequested() {
        TimeSeriesPoint expectedPoint = timeSeriesPoint().timestamp(aTimestamp).measurement("x", 3).build();
        stubFor(get(urlMatching(format(".*/%s/test/hours/last", owner))).willReturn(aResponse().withBody(json(expectedPoint))));
        Optional<TimeSeriesPoint> actualPoint = ingestClient.last(timeSeriesDefinition().name("test").distance(hours));
        assertEquals(expectedPoint, actualPoint.orElse(null));
    }

    @Test
    public void shouldReturnEmptyWhenLastRequestedAndTimeSeriesIsEmpty() {
        stubFor(get(urlMatching(format(".*/%s/test/hours/last", owner))).willReturn(aResponse().withStatus(204)));
        Optional<TimeSeriesPoint> actualPoint = ingestClient.last(timeSeriesDefinition().name("test").distance(hours));
        assertFalse(actualPoint.isPresent());
    }

    private void createStub(int status) {
        stubFor(
                any(urlPathMatching(".*"))
                    .willReturn(aResponse().withStatus(status)));
    }

    private void createWiremockStub(int responseCode) {
        wireMockRule.stubFor(
                any(urlPathMatching(".*")).willReturn(aResponse()
                        .withHeader("Content-Type", "application/json").withStatus(responseCode)));
    }

    private void givenOkResponse(int numberOfPoints) {
        IngestResponse.Builder response = IngestResponse.builder();
        for (int i = 0; i < numberOfPoints; i++)
            response.status(Ok);
        wireMockRule.stubFor(
                any(urlPathMatching(".*"))
                        .willReturn(aResponse()
                                .withHeader("Content-Type", "application/json")
                                .withStatus(HttpURLConnection.HTTP_OK)
                                .withBody(json(response.build()))
                        )
        );
    }

    private static TimeSeriesPoint aPoint() {
        return timeSeriesPoint().timestamp(now()).measurement("m1", 111L).build();
    }

    private TimeSeriesDefinition aSeriesDefinition() {
        return timeSeriesDefinition().name("aSeries").distance(hours);
    }

    private List<TimeSeriesPoint> twoPoints() {
        return asList(
                timeSeriesPoint()
                        .timestamp(now())
                        .measurement("m1", 111L)
                        .measurement("m2", 222L)
                        .build(),
                timeSeriesPoint()
                        .timestamp(now())
                        .measurement("m1", 111L)
                        .build()
        );
    }

    private String json(Object o) {
        try {
            return objectMapper.writer().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
