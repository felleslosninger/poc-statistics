package no.difi.statistics.ingest.client;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.difi.statistics.ingest.client.exception.DataPointAlreadyExists;
import no.difi.statistics.ingest.client.exception.IngestFailed;
import no.difi.statistics.ingest.client.exception.MalformedUrl;
import no.difi.statistics.ingest.client.model.Measurement;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static no.difi.statistics.ingest.client.Distance.minute;

public class IngestClientTest {

    private static final String JSON = "application/json";
    private static final String content_type = "Content-Type";
    private static final String authorization = "Authorization";
    private static final String hostname = "localhost";
    private static final String expected_json = "{\"timestamp\":\"2016-08-03T15:40:04+02:00\",\"measurements\":[{\"id\":\"id1\",\"value\":1},{\"id\":\"id2\",\"value\":2}]}";

    private static final String username = "984661185";
    private static final String password = "123456";
    private static final String series_name = "seriesname";
    private static final int read_timeout = 100;
    private static final int connection_timeout = 5000;

    private static final String owner = "999888777";
    private static final String valid_url = "/999888777/seriesname/minutes";
    private static final int delay_for_timeout = 200;

    private final IngestClient ingestClient;

    private final TimeSeriesPoint timeSeriesPoint;

    public IngestClientTest() throws IOException {
        wireMockRule.start();

        ingestClient = new IngestClient("http://localhost:" + wireMockRule.port(), read_timeout, connection_timeout, owner, username, password);

        timeSeriesPoint = buildValidTimeSeriesPoint();
    }

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig()
            .bindAddress(hostname)
            .dynamicPort());

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void shouldSucceedWhenValidRequestWithAuthorizationForMinute() throws Exception {
        createStub(HttpURLConnection.HTTP_OK);

        ingestClient.ingest(series_name, minute, timeSeriesPoint);

        verify(postRequestedFor(urlEqualTo(valid_url))
                .withHeader(content_type, equalTo(JSON)));
    }

    @Test
    public void shouldSucceedWhenValidRequestWithAuthorizationForHour() throws Exception {
        createStub(HttpURLConnection.HTTP_OK);

        ingestClient.ingest(series_name, minute, timeSeriesPoint);

        verify(postRequestedFor(urlEqualTo(valid_url))
                .withHeader(content_type, equalTo(JSON)));
    }

    @Test
    public void shouldThrowExceptionWhenConnectionTimeoutOccur(){
        wireMockRule.addRequestProcessingDelay(delay_for_timeout);

        expectedEx.expect(IngestFailed.class);
        expectedEx.expectMessage("Could not call IngestService");

        ingestClient.ingest(series_name, minute, timeSeriesPoint);
    }

    @Test
    public void shouldThrowExceptionWhenDatapointAlreadyExists(){
        createStub(HttpURLConnection.HTTP_CONFLICT);

        expectedEx.expect(DataPointAlreadyExists.class);

        ingestClient.ingest(series_name, minute, timeSeriesPoint);
    }

    @Test
    public void shouldThrowExceptionWhenContentTypeIsWrong() {
        createStub(HttpURLConnection.HTTP_UNSUPPORTED_TYPE);

        expectedEx.expect(IngestFailed.class);
        expectedEx.expectMessage("Could not post to Ingest Service");

        ingestClient.ingest(series_name, minute, timeSeriesPoint);
    }

    @Test
    public void shouldGetAuthenticationErrorWhenAuthenticationFails() {
        createStub(HttpURLConnection.HTTP_UNAUTHORIZED);

        expectedEx.expect(IngestFailed.class);
        expectedEx.expectMessage("Failed to authorize Ingest service");

        ingestClient.ingest(series_name, minute, timeSeriesPoint);
    }

    @Test
    public void shouldFailWhenUrlIsWrong() {
        final IngestClient ingestClient = new IngestClient("crappy/url", 150, 5000, owner, username, password);
        expectedEx.expect(MalformedUrl.class);
        expectedEx.expectMessage("Could not create URL to IngestService");

        ingestClient.ingest(series_name, minute, timeSeriesPoint);
    }

    private void createStub(int status) {
        stubFor(
                any(urlPathMatching("([a-zA-Z0-9/])"))
                .withHeader(content_type, equalTo("application/json"))
                .withHeader(authorization, equalTo(validAuthHeader()))
                .withRequestBody(equalToJson(expected_json, JSONCompareMode.NON_EXTENSIBLE))
                .willReturn(aResponse().withStatus(status)));
    }

    private String validAuthHeader(){
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }

    private TimeSeriesPoint buildValidTimeSeriesPoint() {
        List<Measurement> measurement = new ArrayList<>();
        measurement.add(new Measurement("id1", 1));
        measurement.add(new Measurement("id2", 2));

        ZonedDateTime timestamp = ZonedDateTime.parse("2016-08-03T15:40:04.000+02:00");
        return TimeSeriesPoint.builder().measurements(measurement).timestamp(timestamp).build();
    }
}
