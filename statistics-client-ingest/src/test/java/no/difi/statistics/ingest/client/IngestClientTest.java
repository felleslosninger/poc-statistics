package no.difi.statistics.ingest.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.restassured.http.ContentType;
import no.difi.statistics.ingest.client.model.Measurement;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static io.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class IngestClientTest {

    private static final String JSON = "application/json";
    private final IngestClient ingestClient;
    private final IngestClient ingestClientMock;

    private WireMockServer ingestService;
    private HttpURLConnection mockUrlCon;
    private URL url;
    private OutputStream outputstream;
    private MockMvc mockMvc;

    private final TimeSeriesPoint timeSeriesPoint = buildTimeSeriesPoint();

    public IngestClientTest(){
        ingestClient = new IngestClient();
        ingestClientMock = mock(IngestClient.class);
        ingestService = new WireMockServer(wireMockConfig()
                .bindAddress("localhost")
                .dynamicPort());
    }

    @Before
    public void before() throws Exception{
        mockUrlCon = mock(HttpURLConnection.class);
        URLStreamHandler stubUrlHandler = getStubHandler();
        outputstream = mock(OutputStream.class);
        url = new URL("foo", "bar", 99, "/foobar", stubUrlHandler);
        ingestService.start();

        Mockito.when(mockUrlCon.getOutputStream()).thenReturn(outputstream);
        Mockito.when(mockUrlCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    }

    public void setupStub() {
        stubFor(post(urlPathEqualTo("/minutes/seriesName"))
//                .withRequestBody(WireMock.containing("BODY"))
                .withHeader("Content-Type", equalTo(JSON))
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
    public void whenPostingToMinutesWithValidJsonHttpOKShouldBeReturned(){
        setupStub();
        given()
                .contentType(ContentType.JSON)
                .body("{\"key\": \"val\"}")
                .when()
                .post("http://localhost:8080/minutes/seriesName")
                .then()
                .assertThat()
                .statusCode(200);
    }

    @Test
    public void whenCallingPostMinutesCorrectURLIsUsed()throws Exception {

        ArgumentCaptor argumentCaptor = ArgumentCaptor.forClass(URL.class);

//        doNothing().when(ingestClientMock).minute((URL)argumentCaptor.capture(), eq(timeSeriesPoint));
        doCallRealMethod().when(ingestClientMock).minute(anyString(), eq(timeSeriesPoint));

        ingestClientMock.minute("seriesName", timeSeriesPoint);

        assertEquals("http://localhost:8080/minutes/seriesName", argumentCaptor.getValue().toString());
    }

    @Test
    public void whenCallingPostMinutesDoOutputIsSetToTrue() throws Exception {
//        ingestClient.minute(url, timeSeriesPoint);
        verify(mockUrlCon).setDoOutput(true);
    }

    @Test
    public void whenCallingPostMinutesPostIsSetAsRequestMethod() throws Exception{
//        ingestClient.minute(url, timeSeriesPoint);
        verify(mockUrlCon).setRequestMethod("POST");
    }

    @Test
    public void whenCallingPostMinutesContentTypeIsSetToJson() throws Exception {
//        ingestClient.minute(url, timeSeriesPoint);
        verify(mockUrlCon).setRequestProperty("Content-Type", "application/json");
    }

    @Test
    public void whenCallingPostMinutesJsonStringIsAsExpected() throws Exception {
//        ingestClient.minute(url, timeSeriesPoint);

        String expectedJsonString = "{\"timestamp\":\"2016-08-03T15:40:04+02:00\",\"measurements\":[{\"id\":\"id1\",\"value\":1},{\"id\":\"id2\",\"value\":2}]}";
        ArgumentCaptor<byte[]> argumentCaptor = ArgumentCaptor.forClass(byte[].class);

        verify(outputstream).write(argumentCaptor.capture());
        assertEquals(expectedJsonString, new String(argumentCaptor.getValue()));
    }

    @Test(expected = ProtocolException.class)
    public void whenSetRequestMethodThrowsIOExceptionThenExpectException() throws Exception {
        doThrow(new ProtocolException()).when(mockUrlCon).setRequestMethod(anyString());
//        ingestClient.minute(url, timeSeriesPoint);
    }

    @Test(expected = IOException.class)
    public void whenGetOutputstreamThrowsIOExceptionThenExpectException()throws Exception {
        doThrow(new IOException()).when(mockUrlCon).getOutputStream();
//        ingestClient.minute(url, timeSeriesPoint);
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
