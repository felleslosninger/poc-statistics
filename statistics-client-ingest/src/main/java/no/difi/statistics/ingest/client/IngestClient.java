package no.difi.statistics.ingest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.client.exception.IngestException;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class IngestClient implements IngestService {

    private static final String CONTENT_TYPE_KEY = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String SERVICE_NAME = "minutes";
    private static final String REQUEST_METHOD_POST = "POST";

    private final ObjectMapper objectMapper;
    private final JavaTimeModule javaTimeModule;
    private final ISO8601DateFormat iso8601DateFormat;

    private final String serviceURLTemplate;

    public IngestClient(String baseURL) throws MalformedURLException {
        objectMapper = new ObjectMapper();
        javaTimeModule = new JavaTimeModule();
        iso8601DateFormat = new ISO8601DateFormat();
        serviceURLTemplate = baseURL + "/" + SERVICE_NAME + "/%s";
    }

    public void minute(String seriesName, TimeSeriesPoint timeSeriesPoint) throws IngestException {
        URL url;
        try {
            url = new URL(String.format(serviceURLTemplate, seriesName));
        }catch(MalformedURLException e){
            throw new IngestException("Could not create URL to IngestService", e);
        }
        try {
            minute(timeSeriesPoint, url);
        }catch(IOException e){
            throw new IngestException("Could not call IngestService", e);
        }
    }

    private void minute(TimeSeriesPoint timeSeriesPoint, URL url) throws IOException, IngestException {
        HttpURLConnection conn = getConnection(url);
        OutputStream outputStream = writeJsonToOutputStream(timeSeriesPoint, conn);
        outputStream.flush();
        controlResponse(conn);
        conn.disconnect();
    }

    private HttpURLConnection getConnection(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        conn.setRequestMethod(REQUEST_METHOD_POST);
        conn.setRequestProperty(CONTENT_TYPE_KEY, JSON_CONTENT_TYPE);

        return conn;
    }

    private OutputStream writeJsonToOutputStream(TimeSeriesPoint timeSeriesPoint, HttpURLConnection conn) throws IOException {
        OutputStream outputStream = conn.getOutputStream();
        ObjectWriter objectWriter = getObjectWriter();
        String jsonString = objectWriter.writeValueAsString(timeSeriesPoint);
        outputStream.write(jsonString.getBytes());

        return outputStream;
    }

    private ObjectWriter getObjectWriter() {
        return objectMapper
                .registerModule(javaTimeModule)
                .setDateFormat(iso8601DateFormat)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .writerFor(TimeSeriesPoint.class);
    }

    private void controlResponse(HttpURLConnection conn) throws IOException, IngestException {
        int responseCode = conn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new IngestException("Could not post to Ingest Service. Response code from service was " + responseCode);
        }
    }
}
