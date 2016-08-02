package no.difi.statistics.client;

import no.difi.statistics.client.exception.DifiStatisticsException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class IngestClientTest {

    private final IngestClient ingestClient;
    private final IngestClient ingestClientMock;

    private HttpURLConnection mockUrlCon;
    private URL url;
    private OutputStream outputstream;

    private Map<String, Integer> measurement;
    private ZonedDateTime timestamp;

    public IngestClientTest(){
        final HttpHelper httpHelper = new HttpHelper();
        final JsonHelper jsonHelper = new JsonHelper(httpHelper);
        ingestClient = new IngestClient(jsonHelper, httpHelper);
        ingestClientMock = mock(IngestClient.class);
    }

    @Before
    public void before() throws Exception{
        mockUrlCon = mock(HttpURLConnection.class);
        URLStreamHandler stubUrlHandler = getStubHandler();
        outputstream = mock(OutputStream.class);
        url = new URL("foo", "bar", 99, "/foobar", stubUrlHandler);

        measurement = new HashMap<>();
        measurement.put("id1", 1);
        measurement.put("id2", 2);
        timestamp = ZonedDateTime.parse("2016-07-29T13:58:14.640+02:00[Europe/Berlin]");

        Mockito.when(mockUrlCon.getOutputStream()).thenReturn(outputstream);
        Mockito.when(mockUrlCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    }

    @Test
    public void whenCallingPostMinutesCorrectURLIsUsed()throws Exception {

        ArgumentCaptor argumentCaptor = ArgumentCaptor.forClass(URL.class);

        doNothing().when(ingestClientMock).postMinutes((URL)argumentCaptor.capture(), eq(measurement), eq(timestamp));
        doCallRealMethod().when(ingestClientMock).postMinutes(anyString(), eq(measurement), eq(timestamp));

        ingestClientMock.postMinutes("seriesName", measurement, timestamp);

        assertEquals("http://localhost:8080/minutes/seriesName", argumentCaptor.getValue().toString());
    }

    @Test
    public void whenCallingPostMinutesDoOutputIsSetToTrue() throws Exception {
        ingestClient.postMinutes(url, measurement, timestamp);
        verify(mockUrlCon).setDoOutput(true);
    }

    @Test
    public void whenCallingPostMinutesPostIsSetAsRequestMethod() throws Exception{
        ingestClient.postMinutes(url, measurement, timestamp);
        verify(mockUrlCon).setRequestMethod("POST");
    }

    @Test
    public void whenCallingPostMinutesContentTypeIsSetToJson() throws Exception {
        ingestClient.postMinutes(url, measurement, timestamp);
        verify(mockUrlCon).setRequestProperty("Content-Type", "application/json");
    }

    @Test
    public void whenCallingPostMinutesJsonStringIsAsExpected() throws Exception {
        ingestClient.postMinutes(url, measurement, timestamp);

        String expectedJsonString = "{\"timestamp\":1469793494.640000000,\"measurements\":[{\"id\":\"id2\",\"value\":2},{\"id\":\"id1\",\"value\":1}]}";
        ArgumentCaptor<byte[]> argumentCaptor = ArgumentCaptor.forClass(byte[].class);

        verify(outputstream).write(argumentCaptor.capture());
        assertEquals(expectedJsonString, new String(argumentCaptor.getValue()));
    }

    @Test(expected = DifiStatisticsException.class)
    public void whenSetRequestMethodThrowsIOExceptionThenExpectDifiStatisticsException() throws Exception {
        doThrow(new ProtocolException()).when(mockUrlCon).setRequestMethod(anyString());
        ingestClient.postMinutes(url, measurement, timestamp);
    }

    @Test(expected = DifiStatisticsException.class)
    public void whenGetOutputstreamThrowsIOExceptionThenExpectDifiStatisticsException()throws Exception {
        doThrow(new IOException()).when(mockUrlCon).getOutputStream();
        ingestClient.postMinutes(url, measurement, timestamp);
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
