package no.difi.statistics.ingest.client;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import java.util.Base64;
import no.difi.statistics.ingest.client.exception.IngestException;
import no.difi.statistics.ingest.client.model.Measurement;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class IngestClientTest {

    private static final String JSON = "application/json";
    private static final String CONTENTTYPE = "Content-Type";
    private static final String AUTHORIZATION = "Authorization";
    private static final String HOSTNAME = "localhost";
    private static final String SERVICE = "/minutes";
    private static final String SERVICE_REGEX = SERVICE + "/.+";
    private static final String PROTOCOL = "http://";
    private static final String EXPECTED_JSON_STRING = "{\"timestamp\":\"2016-08-03T15:40:04+02:00\",\"measurements\":[{\"id\":\"id1\",\"value\":1},{\"id\":\"id2\",\"value\":2}]}";

    private static final String VALID_USERNAME = "astrid";
    private static final String VALID_PASSWORD = "123456";
    private static final String INVALID_PASSWORD = "123";

    private static final String EXCEPTIONMESSAGE = "Could not call IngestService";

    private final IngestClient ingestClient;

    private final TimeSeriesPoint timeSeriesPoint;

    public IngestClientTest() throws IOException {
        wireMockRule.start();

        ingestClient = new IngestClient(PROTOCOL + HOSTNAME + ":" + wireMockRule.port());

        timeSeriesPoint = buildValidTimeSeriesPoint();
    }

    @Before
    public void before() throws Exception{
        setup400Stub(VALID_USERNAME, VALID_PASSWORD);
        setup415Stub(VALID_USERNAME, VALID_PASSWORD);
        setupMissingAuthorizationHeaderStub();
        setup200Stub(VALID_USERNAME, VALID_PASSWORD);
        setupWrongPasswordStub(VALID_USERNAME, INVALID_PASSWORD);
    }

    private void setup415Stub(String username, String password) {
        stubFor(post(urlPathMatching(SERVICE_REGEX))
                .withHeader(CONTENTTYPE, notMatching(JSON))
                .withHeader(AUTHORIZATION, equalTo(validAuthHeader(username, password)))
                .willReturn(aResponse()
                        .withStatus(415)));
    }

    private void setup400Stub(String username, String password) {
        stubFor(post(urlPathMatching(SERVICE_REGEX))
                .withHeader(CONTENTTYPE,equalTo(JSON))
                .withHeader(AUTHORIZATION, equalTo(validAuthHeader(username, password)))
                .withRequestBody(notMatching("\\{\"timestamp\":.*\"measurements\":.*\\}"))
                .willReturn(aResponse()
                        .withStatus(400)));
    }

    private void setup200Stub(String username, String password) {
        stubFor(post(urlPathMatching(SERVICE_REGEX))
                .withHeader(CONTENTTYPE, equalTo(JSON))
                .withHeader(AUTHORIZATION, equalTo(validAuthHeader(username, password)))
                .withRequestBody(equalToJson(EXPECTED_JSON_STRING, JSONCompareMode.NON_EXTENSIBLE))
                .willReturn(aResponse()
                        .withStatus(200)));
    }

    private void setupMissingAuthorizationHeaderStub() {
        stubFor(post(urlPathMatching(SERVICE_REGEX))
                .withHeader(CONTENTTYPE, equalTo(JSON))
                .withRequestBody(equalToJson(EXPECTED_JSON_STRING))
                .willReturn(aResponse()
                        .withStatus(401)));
    }

    private void setupWrongPasswordStub(String username, String password) {
        stubFor(post(urlPathMatching(SERVICE_REGEX))
                .withHeader(CONTENTTYPE, equalTo(JSON))
                .withHeader(AUTHORIZATION, equalTo(validAuthHeader(username, password)))
                .withRequestBody(equalToJson(EXPECTED_JSON_STRING))
                .willReturn(aResponse()
                        .withStatus(401)));
    }

    private String validAuthHeader(String username, String password){
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }

    private TimeSeriesPoint buildValidTimeSeriesPoint() {
        List<Measurement> measurement = new ArrayList<>();
        measurement.add(new Measurement("id1", 1));
        measurement.add(new Measurement("id2", 2));

        ZonedDateTime timestamp = ZonedDateTime.parse("2016-08-03T15:40:04.000+02:00");
        return TimeSeriesPoint.builder().measurements(measurement).timestamp(timestamp).build();
    }

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig()
            .bindAddress(HOSTNAME)
            .dynamicPort());

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void whenCallingMinutesContentTypeJsonIsSpecified() throws Exception {
        String seriesName = "seriesName3";
        ingestClient.minute(seriesName, timeSeriesPoint);
        verify(postRequestedFor(urlEqualTo(SERVICE + "/" + seriesName))
        .withHeader(CONTENTTYPE, equalTo(JSON)));
    }

    @Test
    public void whenCallingAuthorizationHeaderIsSpecifiedWithValidUsernameAndPassword(){
        String seriesName = "seriesName3";
        ingestClient.minute(seriesName, timeSeriesPoint);
        verify(postRequestedFor(urlEqualTo(SERVICE + "/" + seriesName))
        .withHeader(AUTHORIZATION, equalTo(validAuthHeader(VALID_USERNAME, VALID_PASSWORD))));
    }

    @Test
    public void whenCallingMinutesCorrectURLIsRequested() throws Exception {
        String seriesName = "seriesName3";
        ingestClient.minute(seriesName, timeSeriesPoint);
        verify(postRequestedFor(urlEqualTo(SERVICE + "/" + seriesName)));
    }

    @Test
    public void whenCallingMinutesRequestBodyIsAsExpected() throws Exception {
        String seriesName = "seriesName3";
        ingestClient.minute(seriesName, timeSeriesPoint);
        verify(postRequestedFor(urlEqualTo(SERVICE + "/" + seriesName))
                .withRequestBody(equalToJson(EXPECTED_JSON_STRING)));
    }

    @Test
    public void whenConnectionTimesOutClientThrowsException(){
        wireMockRule.addRequestProcessingDelay(6000);
        expectedEx.expect(IngestException.class);
        expectedEx.expectMessage(EXCEPTIONMESSAGE);
        expectedEx.expectCause(IsInstanceOf.<Throwable>instanceOf(SocketTimeoutException.class));

        ingestClient.minute("name", timeSeriesPoint);
    }
}
