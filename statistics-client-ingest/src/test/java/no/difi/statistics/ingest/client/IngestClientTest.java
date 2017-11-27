package no.difi.statistics.ingest.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.difi.statistics.ingest.client.model.Measurement;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static no.difi.statistics.ingest.client.model.MeasurementDistance.hours;
import static no.difi.statistics.ingest.client.model.MeasurementDistance.minutes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IngestClientTest {

    private static final String JSON = "application/json";
    private static final String content_type = "Content-Type";
    private static final String hostname = "localhost";

    private static final String username = "984661185";
    private static final String password = "123456";
    private static final String series_name = "seriesname";
    private static final int read_timeout = 100;
    private static final int connection_timeout = 5000;

    private static final String owner = "999888777";
    private static final String valid_url = "/999888777/seriesname/minutes";
    private static final int delay_for_timeout = 200;
    private final ZonedDateTime aTimestamp = ZonedDateTime.of(2016, 3, 3, 0, 0, 0, 0, ZoneId.of("UTC"));

    private IngestClient ingestClient;
    private TimeSeriesPoint timeSeriesPoint;
    private ObjectMapper objectMapper;

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig()
            .bindAddress(hostname)
            .dynamicPort());

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void before() throws MalformedURLException {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .setDateFormat(new ISO8601DateFormat())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        ingestClient = new IngestClient(new URL("http://localhost:" + wireMockRule.port()), read_timeout, connection_timeout, owner, username, password);
        timeSeriesPoint = buildValidTimeSeriesPoint();
    }

    @Test
    public void shouldSucceedWhenValidRequestWithAuthorizationForMinute() throws Exception {
        createStub(HttpURLConnection.HTTP_OK);

        ingestClient.ingest(TimeSeriesDefinition.builder().name(series_name).distance(minutes), singletonList(timeSeriesPoint));

        verify(postRequestedFor(urlEqualTo(valid_url))
                .withHeader(content_type, equalTo(JSON)));
    }

    @Test
    public void shouldSucceedWhenValidRequestWithAuthorizationForHour() throws Exception {
        createStub(HttpURLConnection.HTTP_OK);

        ingestClient.ingest(TimeSeriesDefinition.builder().name(series_name).distance(minutes), singletonList(timeSeriesPoint));

        verify(postRequestedFor(urlEqualTo(valid_url))
                .withHeader(content_type, equalTo(JSON)));
    }

    @Test
    public void shouldThrowExceptionWhenConnectionTimeoutOccur(){
        wireMockRule.addRequestProcessingDelay(delay_for_timeout);

        expectedEx.expect(IngestService.ConnectFailed.class);

        ingestClient.ingest(TimeSeriesDefinition.builder().name(series_name).distance(minutes), singletonList(timeSeriesPoint));
    }

    @Test
    public void shouldThrowExceptionWhenDatapointAlreadyExists(){
        createStub(HttpURLConnection.HTTP_CONFLICT);

        expectedEx.expect(IngestService.DataPointAlreadyExists.class);

        ingestClient.ingest(TimeSeriesDefinition.builder().name(series_name).distance(minutes), singletonList(timeSeriesPoint));
    }

    @Test
    public void shouldThrowExceptionWhenContentTypeIsWrong() {
        createStub(HttpURLConnection.HTTP_UNSUPPORTED_TYPE);

        expectedEx.expect(IngestService.Failed.class);
        expectedEx.expectMessage("Ingest failed (415)");

        ingestClient.ingest(TimeSeriesDefinition.builder().name(series_name).distance(minutes), singletonList(timeSeriesPoint));
    }

    @Test
    public void shouldGetAuthenticationErrorWhenAuthenticationFails() {
        createStub(HttpURLConnection.HTTP_UNAUTHORIZED);
        expectedEx.expect(IngestService.Unauthorized.class);
        ingestClient.ingest(TimeSeriesDefinition.builder().name(series_name).distance(minutes), singletonList(timeSeriesPoint));
    }

    @Test
    public void shouldReturnLastWhenLastRequested() {
        TimeSeriesPoint expectedPoint = TimeSeriesPoint.builder().timestamp(aTimestamp).measurement("x", 3).build();
        stubFor(get(urlMatching(format(".*/%s/test/hours/last", owner))).willReturn(aResponse().withBody(json(expectedPoint))));
        Optional<TimeSeriesPoint> actualPoint = ingestClient.last(TimeSeriesDefinition.builder().name("test").distance(hours));
        assertEquals(expectedPoint, actualPoint.orElse(null));
    }

    @Test
    public void shouldReturnEmptyWhenLastRequestedAndTimeSeriesIsEmpty() {
        stubFor(get(urlMatching(format(".*/%s/test/hours/last", owner))).willReturn(aResponse().withStatus(204)));
        Optional<TimeSeriesPoint> actualPoint = ingestClient.last(TimeSeriesDefinition.builder().name("test").distance(hours));
        assertFalse(actualPoint.isPresent());
    }

    private byte[] json(Object object) {
        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException();
        }
    }

    private void createStub(int status) {
        stubFor(
                any(urlPathMatching(".*"))
                    .willReturn(aResponse().withStatus(status)));
    }

    private TimeSeriesPoint buildValidTimeSeriesPoint() {
        List<Measurement> measurement = new ArrayList<>();
        measurement.add(new Measurement("id1", 1));
        measurement.add(new Measurement("id2", 2));

        ZonedDateTime timestamp = ZonedDateTime.parse("2016-08-03T15:40:04.000+02:00");
        return TimeSeriesPoint.builder().measurements(measurement).timestamp(timestamp).build();
    }
}
