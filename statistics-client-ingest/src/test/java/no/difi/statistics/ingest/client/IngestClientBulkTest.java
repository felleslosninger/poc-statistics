package no.difi.statistics.ingest.client;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.difi.statistics.ingest.client.exception.IngestFailed;
import no.difi.statistics.ingest.client.exception.MalformedUrl;
import no.difi.statistics.ingest.client.exception.Unauthorized;
import no.difi.statistics.ingest.client.model.Measurement;
import no.difi.statistics.ingest.client.model.MeasurementDistance;
import no.difi.statistics.ingest.client.model.TimeSeriesDefinition;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.HttpURLConnection;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.time.LocalDateTime.now;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class IngestClientBulkTest {
    //Note: Initialization of IngestClient is tested in IngestClientTest.
    //      This test class is soly for testing bulk transfer through statistics-client-ingest::IngestClient.
    private IngestClient ingestClient;

    private static final String owner = "owner";
    private static final String seriesName = "name";
    private static final String validUrl = "/owner/name/hours";
    private static final String username = "username";
    private static final String password = "password";
    private static final String contentType = "Content-Type";
    private static final LocalDateTime localDateTimeStamp = now();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig()
            .bindAddress("localhost")
            .dynamicPort());
    @Rule public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() {
        wireMockRule.start();

        ingestClient = new IngestClient("http://localhost:" + wireMockRule.port(), 500, 500, owner, username, password);
    }

    @After
    public void tearDown() {
        wireMockRule.stop();
    }

    @Test
    public void shouldSucceedWhenValidRequest() throws Exception {
        createWiremockStub(HttpURLConnection.HTTP_OK);

        ingestClient.ingest(createTimeSeriesDefinitionValid(), createTimeSeriesPointValid());

        wireMockRule.verify(postRequestedFor(urlPathMatching("([a-zA-Z0-9/])"))
                .withHeader(contentType, equalTo("application/json"))
                .withRequestBody(equalTo(createExpectedJson())));
    }

    @Test
    public void shouldFailWithMalformedUrlExceptionWhenUrlIsCrappy() {
        final IngestClient ingestClient = new IngestClient("crappy/url", 150, 5000, owner, username, password);
        expectedEx.expect(MalformedUrl.class);
        expectedEx.expectMessage("Could not create URL to IngestService");

        ingestClient.ingest(createTimeSeriesDefinitionValid(), createTimeSeriesPointValid());
    }

    @Test
    public void shouldFailWithIOExceptionWhenSomethingFailsInTransmission() {
        createWiremockStub(HttpURLConnection.HTTP_OK);
        wireMockRule.addRequestProcessingDelay(501);

        expectedEx.expect(IngestFailed.class);
        expectedEx.expectMessage("Could not open connection to statistics");

        ingestClient.ingest(createTimeSeriesDefinitionValid(), createTimeSeriesPointValid());
    }

    @Test
    public void shouldFailWithUnauthorizedExceptionWhenUnauthorized() {
        createWiremockStub(HttpURLConnection.HTTP_UNAUTHORIZED);

        expectedEx.expect(Unauthorized.class);
        expectedEx.expectMessage("Failed to authorize Ingest service");

        ingestClient.ingest(createTimeSeriesDefinitionValid(), createTimeSeriesPointValid());
    }

    @Test
    public void shouldFailWithUnauthorizedExceptionWhenForbidden() {
        createWiremockStub(HttpURLConnection.HTTP_FORBIDDEN);

        expectedEx.expect(Unauthorized.class);
        expectedEx.expectMessage("Failed to authorize Ingest service");

        ingestClient.ingest(createTimeSeriesDefinitionValid(), createTimeSeriesPointValid());
    }

    @Test
    public void shouldFailWithIngestFailExceptionWhenNotFound() {
        createWiremockStub(HttpURLConnection.HTTP_NOT_FOUND);

        expectedEx.expect(IngestFailed.class);
        expectedEx.expectMessage("Failed, could not find URL you have given");

        ingestClient.ingest(createTimeSeriesDefinitionValid(), createTimeSeriesPointValid());
    }

    private TimeSeriesDefinition createTimeSeriesDefinitionValid() {
        return TimeSeriesDefinition.builder().name(seriesName).distance(MeasurementDistance.hours).owner(owner);
    }

    private static String createExpectedJson() {
        ZonedDateTime zDateTime = ZonedDateTime.of(localDateTimeStamp, ZoneId.of("Europe/Paris"));
        return "[{\"timestamp\":\"" + zDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "\",\"measurements\":[{\"id\":\"name\",\"value\":222}]}," +
                "{\"timestamp\":\"" + zDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "\",\"measurements\":[{\"id\":\"name\",\"value\":111}]}]";
    }

    private List<TimeSeriesPoint> createTimeSeriesPointValid() {
        return asList(TimeSeriesPoint.builder()
                .measurements(asList(createMeasurement(seriesName), createMeasurement(seriesName)))
                .timestamp(ZonedDateTime.of(localDateTimeStamp, ZoneId.systemDefault()))
                .build(),
                TimeSeriesPoint.builder()
                        .measurements(singletonList(createMeasurement(seriesName)))
                        .timestamp(ZonedDateTime.of(localDateTimeStamp, ZoneId.systemDefault()))
                        .build());
    }

    private static Measurement createMeasurement(String id) {
        return new Measurement(id, 111L);
    }

    private void createWiremockStub(int responseCode) {
        wireMockRule.stubFor(
                any(urlPathMatching(validUrl)).willReturn(aResponse()
                        .withHeader("Content-Type", "application/json").withStatus(responseCode)));
    }
}