package no.difi.statistics.ingest.client;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import no.difi.statistics.ingest.client.exception.IngestException;
import no.difi.statistics.ingest.client.model.Measurement;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.*;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.mockito.Mockito.*;

public class IngestClientTest {

    private static final String JSON = "application/json";
    private static final String CONTENTTYPE = "Content-Type";
    private static final String HOSTNAME = "localhost";
    private static final String SERVICE = "/minutes";
    private static final String EXCEPTIONMESSAGE = "Could not call IngestService";
    private static final String WRONGHTTPCODEMESSAGE = "Could not post to Ingest Service. Response code from service was";
    private static final String ERRORMESSAGE500 = "Should give 500 error";
    private static final String ERRORMESSAGE400 = "Should give 400 error";

    private final IngestClient ingestClientStubbedURL;
    private final IngestClient ingestClientMock;

    private final HttpURLConnection mockUrlCon;

    private final TimeSeriesPoint timeSeriesPoint;
    private final TimeSeriesPoint timeSeriesPointError;
    private final TimeSeriesPoint timeSeriesPoint400Error;

    public IngestClientTest() throws MalformedURLException {
        wireMockRule.start();

        mockUrlCon = mock(HttpURLConnection.class);
        ingestClientStubbedURL = new IngestClient(setupURL());
        ingestClientMock = mock(IngestClient.class);

        timeSeriesPoint = buildValidTimeSeriesPoint();
        timeSeriesPointError = buildTimeSeriesPointWith500ErrorMessage();
        timeSeriesPoint400Error = buildTimeSeriesPointWith400ErrorMessage();
    }

    private URL setupURL() throws MalformedURLException{
        URLStreamHandler stubUrlHandler = getStubHandler();
        return new URL("http", HOSTNAME, wireMockRule.port(), SERVICE + "/", stubUrlHandler);
    }

    @Before
    public void before() throws Exception{
        setupStubs();
    }

    private void setupStubs() {
        setup200Stub();
        setup500Stub();
        setup400Stub();
    }

    private void setup400Stub() {
        stubFor(post(urlPathMatching(SERVICE+"/.+"))
                .withHeader(CONTENTTYPE,equalTo(JSON))
                .withRequestBody(containing(ERRORMESSAGE400))
                .willReturn(aResponse()
                        .withStatus(400)));
    }

    private void setup500Stub() {
        stubFor(post(urlPathMatching(SERVICE+"/.+"))
                .withHeader(CONTENTTYPE,equalTo(JSON))
                .withRequestBody(containing(ERRORMESSAGE500))
                .willReturn(aResponse()
                        .withStatus(500)));
    }

    public void setup200Stub() {
        String expectedJsonString = "{\"timestamp\":\"2016-08-03T15:40:04+02:00\",\"measurements\":[{\"id\":\"id1\",\"value\":1},{\"id\":\"id2\",\"value\":2}]}";
        stubFor(post(urlPathMatching(SERVICE+"/.+"))
                .withHeader(CONTENTTYPE,equalTo(JSON))
                .withRequestBody(equalTo(expectedJsonString))
                .willReturn(aResponse()
                        .withStatus(200)));
    }

    private TimeSeriesPoint buildValidTimeSeriesPoint() {
        List<Measurement> measurement = new ArrayList<>();
        measurement.add(new Measurement("id1", 1));
        measurement.add(new Measurement("id2", 2));

        ZonedDateTime timestamp = ZonedDateTime.parse("2016-08-03T15:40:04.000+02:00");
        return TimeSeriesPoint.builder().measurements(measurement).timestamp(timestamp).build();
    }

    private TimeSeriesPoint buildTimeSeriesPointWith500ErrorMessage() {
        List<Measurement> measurement = new ArrayList<>();
        measurement.add(new Measurement(ERRORMESSAGE500, 1));
        measurement.add(new Measurement("id2", 2));

        ZonedDateTime timestamp = ZonedDateTime.parse("2016-08-03T15:40:04.000+02:00");
        return TimeSeriesPoint.builder().measurements(measurement).timestamp(timestamp).build();
    }

    private TimeSeriesPoint buildTimeSeriesPointWith400ErrorMessage() {
        List<Measurement> measurement = new ArrayList<>();
        measurement.add(new Measurement(ERRORMESSAGE400, 1));
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
    public void whenCallingMinutesCorrectURLIsRequested() throws Exception {
        String seriesName = "seriesName3";
        ingestClientStubbedURL.minute(seriesName, timeSeriesPoint);
        verify(postRequestedFor(urlEqualTo(SERVICE + "/" + seriesName))
                .withHeader(CONTENTTYPE, equalTo(JSON)));
    }

    @Test
    public void whenServerReturns500ErrorClientThrowsException(){
        expectedEx.expect(IngestException.class);
        expectedEx.expectMessage(WRONGHTTPCODEMESSAGE + " 500");
        ingestClientStubbedURL.minute("testError", timeSeriesPointError);
    }

    @Test
    public void whenServerReturnsMalformedRequestClientThrowsException(){
        expectedEx.expect(IngestException.class);
        expectedEx.expectMessage(WRONGHTTPCODEMESSAGE + " 400");
        ingestClientStubbedURL.minute("test400Error", timeSeriesPoint400Error);
    }

    @Test
    public void whenConnectionTimesOutClientThrowsException(){
        wireMockRule.addRequestProcessingDelay(6000);

        expectedEx.expect(IngestException.class);
        expectedEx.expectMessage(EXCEPTIONMESSAGE);
        expectedEx.expectCause(IsInstanceOf.<Throwable>instanceOf(SocketTimeoutException.class));

        ingestClientStubbedURL.minute("name", timeSeriesPoint);
    }

    @Test
    public void whenSetRequestMethodThrowsIOExceptionThenExpectException() throws Exception {
        when(ingestClientMock.getURL()).thenReturn(setupURL());
        doCallRealMethod().when(ingestClientMock).minute(anyString(), anyObject());
        doThrow(new ProtocolException()).when(mockUrlCon).setRequestMethod(anyString());

        expectedEx.expect(IngestException.class);
        expectedEx.expectMessage(EXCEPTIONMESSAGE);

        ingestClientMock.minute("test", timeSeriesPoint);
    }

    @Test
    public void whenGetOutputstreamThrowsIOExceptionThenExpectException()throws Exception {
        when(ingestClientMock.getURL()).thenReturn(setupURL());
        doCallRealMethod().when(ingestClientMock).minute(anyString(), anyObject());
        doThrow(new IOException()).when(mockUrlCon).getOutputStream();

        expectedEx.expect(IngestException.class);
        expectedEx.expectMessage(EXCEPTIONMESSAGE);

        ingestClientMock.minute("test", timeSeriesPoint);
    }

    private URLStreamHandler getStubHandler() {
        return new URLStreamHandler() {
            @Override
            protected HttpURLConnection openConnection(URL u) throws IOException {
                return mockUrlCon;
            }
        };
    }
}
