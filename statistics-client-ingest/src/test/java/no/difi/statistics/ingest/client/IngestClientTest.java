package no.difi.statistics.ingest.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestListener;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import no.difi.statistics.ingest.client.exception.IngestException;
import no.difi.statistics.ingest.client.model.Measurement;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.*;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class IngestClientTest {

    private static final String JSON = "application/json";
    private static final String hostname = "localhost";
    private static final String service = "/minutes";

    private final IngestClient ingestClient;
    private final IngestClient ingestClientMockedURL;

    private WireMockServer ingestService;
    private HttpURLConnection mockUrlCon;

    private final TimeSeriesPoint timeSeriesPoint = buildTimeSeriesPoint();

    public IngestClientTest() throws MalformedURLException {
        ingestClient = new IngestClient();
        ingestClientMockedURL = new IngestClient(setupURL());

        ingestService = setupIngestService();
        mockUrlCon = mock(HttpURLConnection.class);
    }

    private URL setupURL() throws MalformedURLException{
        URLStreamHandler stubUrlHandler = getStubHandler();
        return new URL("http", hostname, 8080, service, stubUrlHandler);
    }

    private WireMockServer setupIngestService() {
        return new WireMockServer(wireMockConfig()
                .bindAddress(hostname)
                .dynamicPort());
    }

    @Before
    public void before() throws Exception{
        ingestService.start();
        setupStub();
    }

    public void setupStub() {
        String expectedJsonString = "{\"timestamp\":\"2016-08-03T15:40:04+02:00\",\"measurements\":[{\"id\":\"id1\",\"value\":1},{\"id\":\"id2\",\"value\":2}]}";
        stubFor(post(urlPathMatching(service+"/.+"))
                .withHeader("Content-Type",equalTo(JSON))
                .withRequestBody(equalTo(expectedJsonString))
                .willReturn(aResponse()
                        .withStatus(200)));
    }

    private TimeSeriesPoint buildTimeSeriesPoint() {
        List<Measurement> measurement = new ArrayList<>();
        measurement.add(new Measurement("id1", 1));
        measurement.add(new Measurement("id2", 2));

        ZonedDateTime timestamp = ZonedDateTime.parse("2016-08-03T15:40:04.000+02:00");
        return TimeSeriesPoint.builder().measurements(measurement).timestamp(timestamp).build();
    }

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8080);

    @Test
    public void whenPostingToMinutesWithValidTimeSeriesPointAndJsonHeaderHttpOKShouldBeReturned() {
        ingestClient.minute("name", timeSeriesPoint);
    }

    @Test
    public void whenCallingMinutesCorrectURLIsRequested() throws Exception {
        List<Request> requests = new ArrayList<>();
           wireMockRule.addMockServiceRequestListener(new RequestListener() {
            @Override
            public void requestReceived(Request request, Response response) {
                requests.add(LoggedRequest.createFrom(request));
            }
        });

        String baseURL = service+"/";
        String seriesName = "seriesName3";
        ingestClient.minute(seriesName, timeSeriesPoint);
        for (Request request : requests){
            assertEquals(request.getUrl(), baseURL + seriesName);
        }
    }

    @Test(expected = IngestException.class)
    public void whenSetRequestMethodThrowsIOExceptionThenExpectException() throws Exception {
        doThrow(new ProtocolException()).when(mockUrlCon).setRequestMethod(anyString());
        ingestClientMockedURL.minute("test", timeSeriesPoint);
    }

    @Test(expected = IngestException.class)
    public void whenGetOutputstreamThrowsIOExceptionThenExpectException()throws Exception {
        doThrow(new IOException()).when(mockUrlCon).getOutputStream();
        ingestClientMockedURL.minute("test", timeSeriesPoint);
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
