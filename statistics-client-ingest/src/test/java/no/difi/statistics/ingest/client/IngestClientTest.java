package no.difi.statistics.ingest.client;

import no.difi.statistics.ingest.client.model.Measurement;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLStreamHandler;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class IngestClientTest {

    private final IngestClient ingestClient;
    private final IngestClient ingestClientMock;

    private HttpURLConnection mockUrlCon;
    private URL url;
    private OutputStream outputstream;

    private final TimeSeriesPoint timeSeriesPoint = buildTimeSeriesPoint();

    public IngestClientTest(){
        ingestClient = new IngestClient();
        ingestClientMock = mock(IngestClient.class);
    }

    @Before
    public void before() throws Exception{
        mockUrlCon = mock(HttpURLConnection.class);
        URLStreamHandler stubUrlHandler = getStubHandler();
        outputstream = mock(OutputStream.class);
        url = new URL("foo", "bar", 99, "/foobar", stubUrlHandler);

        Mockito.when(mockUrlCon.getOutputStream()).thenReturn(outputstream);
        Mockito.when(mockUrlCon.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    }

    private TimeSeriesPoint buildTimeSeriesPoint() {
        List<Measurement> measurement = new ArrayList<>();
        measurement.add(new Measurement("id1", 1));
        measurement.add(new Measurement("id2", 2));

        ZonedDateTime timestamp = ZonedDateTime.parse("2016-08-03T15:40:04.000+02:00");
        return TimeSeriesPoint.builder().measurements(measurement).timestamp(timestamp).build();
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
