package no.difi.statistics.ingest.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.client.model.IngestResponse;
import no.difi.statistics.ingest.client.model.TimeSeriesDefinition;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.List;
import java.util.Optional;

import static java.net.HttpURLConnection.*;

public class IngestClient implements IngestService {

    private static final String CONTENT_TYPE_KEY = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String AUTHORIZATION_KEY = "Authorization";
    private static final String AUTH_METHOD = "Bearer";

    private final ObjectWriter requestWriter;
    private final ObjectReader responseReader;
    private final ObjectReader lastResponseReader;

    private final URL baseUrl;
    private final String owner;
    private final int readTimeoutMillis;
    private final int connectionTimeoutMillis;

    public IngestClient(URL baseURL, int readTimeoutMillis, int connectionTimeoutMillis, String owner) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new Jdk8Module())
                .setDateFormat(new ISO8601DateFormat())
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.requestWriter = objectMapper.writerFor(new TypeReference<List<TimeSeriesPoint>>() {
        });
        this.responseReader = objectMapper.readerFor(IngestResponse.class);
        this.lastResponseReader = objectMapper.readerFor(TimeSeriesPoint.class);
        this.baseUrl = baseURL;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.readTimeoutMillis = readTimeoutMillis;
        this.owner = owner;
    }

    @Override
    public IngestResponse ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints, String token) {
        if(token == null || token.isEmpty()){
            throw new Unauthorized("Access token is null or emtpy. An valid access token from Maskinporten must be provided.");
        }
        HttpURLConnection connection = getConnection(ingestUrlFor(seriesDefinition), "POST", token);
        writeRequest(dataPoints, connection);
        handleResponseCode(connection);
        return readResponse(connection);
    }

    private IngestResponse readResponse(HttpURLConnection connection) {
        try {
            return responseReader.readValue(connection.getInputStream());
        } catch (IOException e) {
            throw new Failed("Response could not be read", e);
        }
    }

    @Override
    public Optional<TimeSeriesPoint> last(TimeSeriesDefinition seriesDefinition) {
        return getFrom(lastUrlFor(seriesDefinition));
    }

    private URL ingestUrlFor(TimeSeriesDefinition seriesDefinition) {
        return url(format("%s/%s/%s/%s", seriesDefinition));
    }

    private URL lastUrlFor(TimeSeriesDefinition seriesDefinition) {
        return url(format("%s/%s/%s/%s/last", seriesDefinition));
    }

    private String format(String template, TimeSeriesDefinition seriesDefinition) {
        return String.format(template, this.baseUrl, this.owner, seriesDefinition.getName(), seriesDefinition.getDistance().name());
    }

    private URL url(String s) {
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new MalformedUrl(e);
        }
    }

    private void handleResponseCode(HttpURLConnection connection) {
        int responseCode;
        try {
            responseCode = connection.getResponseCode();
        } catch (IOException e) {
            throw new Failed("Could not read response code", e);
        }
        switch (responseCode) {
            case HTTP_OK:
            case HTTP_CREATED:
                break;
            case HTTP_CONFLICT:
                throw new DataPointAlreadyExists();
            case HTTP_UNAUTHORIZED:
            case HTTP_FORBIDDEN:
                throw new Unauthorized("Failed to authorize Ingest service (" + responseCode + ")");
            case HTTP_NOT_FOUND:
                throw new Failed("Not found");
            default:
                throw new Failed("Ingest failed (" + responseCode + ")");
        }
    }

    private Optional<TimeSeriesPoint> getFrom(URL url) {
        HttpURLConnection connection = null;
        try {
            connection = getConnection(url, "GET", null);
            if (connection.getResponseCode() == 204)
                return Optional.empty();
            if (connection.getResponseCode() != 200)
                throw new Failed(String.format(
                        "Failed to get response from ingest service [%d %s] on URL: %s",
                        connection.getResponseCode(),
                        connection.getResponseMessage(),
                        url
                ));
            return Optional.of(lastResponseReader.readValue(connection.getInputStream()));
        } catch (IOException e) {
            throw new Failed("Failed to get last point", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private HttpURLConnection getConnection(URL url, String requestMethod, final String token) {
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
            throw new ConnectFailed(e);
        }
        conn.setRequestProperty(CONTENT_TYPE_KEY, JSON_CONTENT_TYPE);
        if (token != null) {
            conn.setRequestProperty(AUTHORIZATION_KEY, AUTH_METHOD + " " + token);
        }
        try {
            conn.connect(); // Connect early. Otherwise will be called implicitly later.
        } catch (IOException e) {
            throw new ConnectFailed(e);
        }
        return conn;
    }

    private void writeRequest(List<TimeSeriesPoint> requestData, HttpURLConnection connection) {
        try {
            requestWriter.writeValue(connection.getOutputStream(), requestData);
        } catch (IOException e) {
            throw new Failed("Could not write request", e);
        }
    }

}
