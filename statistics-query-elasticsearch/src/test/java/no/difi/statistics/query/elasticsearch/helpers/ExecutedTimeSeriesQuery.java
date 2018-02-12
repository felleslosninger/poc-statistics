package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;

public class ExecutedTimeSeriesQuery {

    private TimeSeriesQuery attributes;
    public static TestRestTemplate restTemplate;
    public static ObjectMapper objectMapper;

    ExecutedTimeSeriesQuery(TimeSeriesQuery attributes) {
        this.attributes = attributes;
    }

    public List<TimeSeriesPoint> execute() {
        ResponseEntity<String> response = restTemplate.exchange(
                url(),
                HttpMethod.GET,
                null,
                String.class,
                parameters()
        );
        if (response.getStatusCodeValue() != 200)
            throw new RuntimeException(response.getStatusCodeValue() + (response.getBody() != null ? "/" + response.getBody() : ""));
        else {
            try {
                return deserialize(response.getBody());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected List<TimeSeriesPoint> deserialize(String response) throws IOException {
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response);
    }

    protected String url() {
        return "/{owner}/{series}/{distance}" + queryUrl();
    }

    private Map<String, Object> parameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("owner", attributes.owner());
        parameters.put("series", attributes.name());
        parameters.put("distance", attributes.distance());
        parameters.putAll(queryParameters());
        return parameters;
    }

    protected Map<String, Object> queryParameters() {
        Map<String, Object> parameters = new HashMap<>();
        if (attributes.from() != null) parameters.put("from", formatTimestamp(attributes.from()));
        if (attributes.to() != null) parameters.put("to", formatTimestamp(attributes.to()));
        if (attributes.categories() != null && !attributes.categories().isEmpty())
            parameters.put("categories", attributes.categories().entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(joining(",")));
        return parameters;
    }

    String queryUrl() {
        String s = parameters().keySet().stream()
                .filter(p -> queryParameters().keySet().contains(p))
                .map(p -> p + "=" + "{" + p + "}")
                .collect(joining("&"));
        return s.isEmpty() ? s : "?" + s;
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

}
