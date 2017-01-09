package no.difi.statistics.ingest.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.client.model.TimeSeriesDefinition;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Base64;
import java.util.List;

import static java.lang.String.format;
import static java.net.HttpURLConnection.*;

public class IngestClient implements IngestService {

    private static final String CONTENT_TYPE_KEY = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String AUTHORIZATION_KEY = "Authorization";
    private static final String AUTH_METHOD = "Basic";

    private final ObjectMapper objectMapper;

    private final String username;
    private final String password;
    private final URL baseUrl;
    private final String owner;
    private final int readTimeoutMillis;
    private final int connectionTimeoutMillis;

    public IngestClient(URL baseURL, int readTimeoutMillis, int connectionTimeoutMillis, String owner, String username, String password) throws MalformedUrl {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .setDateFormat(new ISO8601DateFormat())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.baseUrl = baseURL;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.owner = owner;
        this.username = username;
        this.password = password;
    }

    @Override
    public void ingest(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint timeSeriesPoint) {
        postTo(postUrlFor(seriesDefinition), timeSeriesPoint);
    }

    @Override
    public void ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints) {
        try {
            final HttpURLConnection connection = getConnection(postUrlFor(seriesDefinition), "POST");
            writeJsonListToOutputStream(dataPoints, connection);
            handleResponse(connection.getResponseCode());
        } catch (IOException e) {
            throw new ConnectFailed(e);
        }
    }

    @Override
    public TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition) {
        return getFrom(getUrlFor(seriesDefinition));
    }

    private URL postUrlFor(TimeSeriesDefinition seriesDefinition) {
        return postUrl(seriesDefinition.getName(), seriesDefinition.getDistance().name());
    }

    private URL getUrlFor(TimeSeriesDefinition seriesDefinition) {
        return getUrl(seriesDefinition.getName(), seriesDefinition.getDistance().name());
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
                throw new Failed("Failed, could not find URL you have given");
        }
    }

    private void postTo(URL url, TimeSeriesPoint timeSeriesPoint) {
        HttpURLConnection connection = null;
        try {
            connection = getConnection(url, "POST");
            writeJsonToOutputStream(timeSeriesPoint, connection);
            switch (connection.getResponseCode()) {
                case HTTP_OK:
                    break;
                case HTTP_CONFLICT:
                    throw new DataPointAlreadyExists();
                case HTTP_UNAUTHORIZED:
                    throw new Unauthorized("Failed to authorize Ingest service");
                default:
                    throw new Failed("Could not post to Ingest Service");
            }
        } catch (IOException e) {
            throw new Failed("Could not call IngestService", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private TimeSeriesPoint getFrom(URL url) {
        HttpURLConnection connection = null;
        try {
            connection = getConnection(url, "GET");
            if (connection.getResponseCode() != 200)
                throw new Failed(format(
                        "Failed to get response from ingest service [%d %s]",
                        connection.getResponseCode(),
                        connection.getResponseMessage()
                ));
            ObjectReader reader = objectMapper.readerFor(TimeSeriesPoint.class);
            return reader.readValue(connection.getInputStream());
        } catch (IOException e) {
            throw new Failed("Failed to get last point", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private HttpURLConnection getConnection(URL url, String requestMethod) {
        HttpURLConnection conn;
        try {
            conn = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            throw new ConnectFailed(e);
        }
        conn.setDoOutput(true);
        conn.setConnectTimeout(connectionTimeoutMillis);
        conn.setReadTimeout(readTimeoutMillis);
        try {
            conn.setRequestMethod(requestMethod);
        } catch (ProtocolException e) {
            throw new Failed("Unexpected error", e);
        }
        conn.setRequestProperty(CONTENT_TYPE_KEY, JSON_CONTENT_TYPE);
        conn.setRequestProperty(AUTHORIZATION_KEY, AUTH_METHOD + " " + createBase64EncodedCredentials());
        return conn;
    }

    private String createBase64EncodedCredentials(){
        return Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
    }

    private void writeJsonToOutputStream(TimeSeriesPoint timeSeriesPoint, HttpURLConnection conn) throws IOException {
        OutputStream outputStream = conn.getOutputStream();
        ObjectWriter objectWriter = objectMapper.writerFor(TimeSeriesPoint.class);
        String jsonString = objectWriter.writeValueAsString(timeSeriesPoint);
        outputStream.write(jsonString.getBytes());
        outputStream.flush();
    }

    private void writeJsonListToOutputStream(List<TimeSeriesPoint> timeSeriesPoint, HttpURLConnection conn) throws IOException {
        OutputStream stream = conn.getOutputStream();
        ObjectWriter writer = objectMapper.writerFor(List.class);
        String jsonString = writer.writeValueAsString(timeSeriesPoint);
        stream.write(jsonString.getBytes());
        stream.flush();
    }

    private URL postUrl(String seriesName, String distance) {
        try {
            return new URL(format("%s/%s/%s/%s", this.baseUrl, this.owner, seriesName, distance));
        } catch (MalformedURLException e) {
            throw new MalformedUrl(e);
        }
    }

    private URL getUrl(String seriesName, String distance) {
        try {
            return new URL(format("%s/%s/%s/%s/last", this.baseUrl, this.owner, seriesName, distance));
        } catch (MalformedURLException e) {
            throw new MalformedUrl(e);
        }
    }

}
