package no.difi.statistics.ingest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.client.exception.*;
import no.difi.statistics.ingest.client.model.TimeSeriesDefinition;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;
import java.util.List;

import static java.net.HttpURLConnection.*;

public class IngestClient implements IngestService {
    private static final String CONTENT_TYPE_KEY = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String REQUEST_METHOD_POST = "POST";
    private static final String AUTHORIZATION_KEY = "Authorization";
    private static final String AUTH_METHOD = "Basic";

    private final ObjectMapper objectMapper;
    private final JavaTimeModule javaTimeModule;
    private final ISO8601DateFormat iso8601DateFormat;

    private final String username;
    private final String password;
    private final String baseUrl;
    private final String owner;
    private final int readTimeoutMillis;
    private final int connectionTimeoutMillis;

    public IngestClient(String baseURL, int readTimeoutMillis, int connectionTimeoutMillis, String owner, String username, String password) throws MalformedUrl {
        objectMapper = new ObjectMapper();
        javaTimeModule = new JavaTimeModule();
        iso8601DateFormat = new ISO8601DateFormat();
        this.baseUrl = baseURL;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.owner = owner;
        this.username = username;
        this.password = password;
    }

    public void ingest(String seriesName, Distance distance, TimeSeriesPoint timeSeriesPoint) {
        if (distance == Distance.minute) {
            minute(seriesName, timeSeriesPoint);
        }
        else if (distance == Distance.hour) {
            hour(seriesName, timeSeriesPoint);
        }
    }

    public void ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints) {
        try {
            final URL url = new URL(serviceUrlTemplate(seriesDefinition.getName(), seriesDefinition.getDistance().name()));
            final HttpURLConnection connection = getConnection(url);
            writeJsonListToOutputStream(dataPoints, connection);
            handleResponse(connection.getResponseCode());
        } catch (MalformedURLException e) {
            throw new MalformedUrl("Could not create URL to IngestService", e);
        } catch (IOException e) {
            throw new IngestFailed("Could not open connection to statistics", e);
        }
    }

    private void handleResponse(int responseCode) throws IOException {
        switch (responseCode) {
            case HTTP_OK:
            case HTTP_CREATED:
                break;
            case HTTP_UNAUTHORIZED:
            case HTTP_FORBIDDEN:
                throw new Unauthorized("Failed to authorize Ingest service");
            case HTTP_NOT_FOUND:
                throw new IngestFailed("Failed, could not find URL you have given");
        }
    }

    private void minute(String seriesName, TimeSeriesPoint timeSeriesPoint) throws MalformedUrl {
        try {
            final URL url = new URL(serviceUrlTemplate(seriesName, Distance.minute.getValue()));
            dataPoint(timeSeriesPoint, url);
        } catch(MalformedURLException e) {
            throw new MalformedUrl("Could not create URL to IngestService", e);
        }
    }

    private void hour(String seriesName, TimeSeriesPoint timeSeriesPoint) {
        try {
            URL url = new URL(serviceUrlTemplate(seriesName, Distance.hour.getValue()));
            dataPoint(timeSeriesPoint, url);
        } catch(MalformedURLException e) {
            throw new MalformedUrl("Could not create URL to IngestService", e);
        }
    }

    private void dataPoint(TimeSeriesPoint timeSeriesPoint, URL url) {
        HttpURLConnection connection = null;
        try {
            connection = getConnection(url);
            writeJsonToOutputStream(timeSeriesPoint, connection);
            switch (connection.getResponseCode()) {
                case HTTP_OK:
                    break;
                case HTTP_CONFLICT:
                    throw new DataPointAlreadyExists();
                case HTTP_UNAUTHORIZED:
                    throw new Unauthorized("Failed to authorize Ingest service");
                default:
                    throw new IngestFailed("Could not post to Ingest Service");
            }
        } catch (IOException e) {
            throw new IngestFailed("Could not call IngestService", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private HttpURLConnection getConnection(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setConnectTimeout(connectionTimeoutMillis);
        conn.setReadTimeout(readTimeoutMillis);
        conn.setRequestMethod(REQUEST_METHOD_POST);
        conn.setRequestProperty(CONTENT_TYPE_KEY, JSON_CONTENT_TYPE);
        conn.setRequestProperty(AUTHORIZATION_KEY, AUTH_METHOD + " " + createBase64EncodedCredentials());

        return conn;
    }

    private String createBase64EncodedCredentials(){
        return Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }

    private void writeJsonToOutputStream(TimeSeriesPoint timeSeriesPoint, HttpURLConnection conn) throws IOException {
        OutputStream outputStream = conn.getOutputStream();
        ObjectWriter objectWriter = getObjectWriterForTimeSeriesPoint();
        String jsonString = objectWriter.writeValueAsString(timeSeriesPoint);
        outputStream.write(jsonString.getBytes());
        outputStream.flush();
    }

    private void writeJsonListToOutputStream(List<TimeSeriesPoint> timeSeriesPoint, HttpURLConnection conn) throws IOException {
        OutputStream stream = conn.getOutputStream();
        ObjectWriter writer = getObjectWriterForTimeSeriesPointList();

        String jsonString = writer.writeValueAsString(timeSeriesPoint);
        stream.write(jsonString.getBytes());
        stream.flush();
    }

    private ObjectWriter getObjectWriterForTimeSeriesPoint() {
        return objectMapper
                .registerModule(javaTimeModule)
                .setDateFormat(iso8601DateFormat)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .writerFor(TimeSeriesPoint.class);
    }

    private ObjectWriter getObjectWriterForTimeSeriesPointList() {
        return objectMapper
                .registerModule(javaTimeModule)
                .setDateFormat(iso8601DateFormat)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .writerFor(List.class);
    }

    private String serviceUrlTemplate(String seriesName, String distance) {
        return String.format("%s/%s/%s/%s", this.baseUrl, this.owner, seriesName, distance);
    }
}
