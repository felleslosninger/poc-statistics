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
import java.util.Map;
import java.util.Optional;

import static java.net.HttpURLConnection.*;
import static java.util.stream.Collectors.joining;

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
    public void ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints) {
        try {
            HttpURLConnection connection = getConnection(ingestUrlFor(seriesDefinition), "POST");
            writeJsonListToOutputStream(dataPoints, connection);
            handleResponse(connection.getResponseCode());
        } catch (IOException e) {
            throw new ConnectFailed(e);
        }
    }

    @Override
    public Optional<TimeSeriesPoint> last(TimeSeriesDefinition seriesDefinition) {
        return getFrom(lastUrlFor(seriesDefinition));
    }

    private URL ingestUrlFor(TimeSeriesDefinition seriesDefinition) {
        return url(format("%s/%s/%s/%s", seriesDefinition) + categoriesAsString("?", seriesDefinition));
    }

    private URL lastUrlFor(TimeSeriesDefinition seriesDefinition) {
        return url(format("%s/%s/%s/%s/last", seriesDefinition) + categoriesAsString("?", seriesDefinition));
    }

    private String categoriesAsString(String prefix, TimeSeriesDefinition seriesDefinition) {
        return seriesDefinition.getCategories().map(cs -> prefix + categoriesAsString(cs)).orElse("");
    }

    private String categoriesAsString(Map<String, String> categories) {
        return categories.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).sorted().collect(joining("&"));
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

    private void handleResponse(int responseCode) throws IOException {
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
            connection = getConnection(url, "GET");
            if (connection.getResponseCode() == 204)
                return Optional.empty();
            if (connection.getResponseCode() != 200)
                throw new Failed(String.format(
                        "Failed to get response from ingest service [%d %s]",
                        connection.getResponseCode(),
                        connection.getResponseMessage()
                ));
            ObjectReader reader = objectMapper.readerFor(TimeSeriesPoint.class);
            return Optional.of(reader.readValue(connection.getInputStream()));
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

    private void writeJsonListToOutputStream(List<TimeSeriesPoint> timeSeriesPoint, HttpURLConnection conn) throws IOException {
        OutputStream stream = conn.getOutputStream();
        ObjectWriter writer = objectMapper.writerFor(List.class);
        String jsonString = writer.writeValueAsString(timeSeriesPoint);
        stream.write(jsonString.getBytes());
        stream.flush();
    }

}
