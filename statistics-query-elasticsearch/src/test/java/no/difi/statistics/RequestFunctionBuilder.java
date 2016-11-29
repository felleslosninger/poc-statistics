package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Supplier;

import static no.difi.statistics.model.RelationalOperator.*;
import static org.junit.Assert.assertEquals;

public class RequestFunctionBuilder {

    private String owner = "test_owner"; // Index names must be lower case in Elasticsearch
    private String series;
    private String measurementId;
    private MeasurementDistance distance;
    private ZonedDateTime from;
    private ZonedDateTime to;
    private TestRestTemplate restTemplate;
    private ObjectMapper objectMapper;

    public RequestFunctionBuilder(TestRestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public RequestFunctionBuilder from(String series) {
        this.series = series;
        return this;
    }

    public RequestFunctionBuilder from(ZonedDateTime from) {
        this.from = from;
        return this;
    }

    public RequestFunctionBuilder to(ZonedDateTime to) {
        this.to = to;
        return this;
    }

    public RequestFunctionBuilder withDistance(MeasurementDistance distance) {
        this.distance = distance;
        return this;
    }

    public RequestFunctionBuilder withMeasurement(String measurementId) {
        this.measurementId = measurementId;
        return this;
    }

    public Supplier<List<TimeSeriesPoint>> lessThanPercentile(int percentile) {
        return relationalToPercentile(percentile, lt);
    }

    public Supplier<List<TimeSeriesPoint>> greaterThanPercentile(int percentile) {
        return relationalToPercentile(percentile, gt);
    }

    public Supplier<List<TimeSeriesPoint>> lessThanOrEqualToPercentile(int percentile) {
        return relationalToPercentile(percentile, lte);
    }

    public Supplier<List<TimeSeriesPoint>> greaterThanOrEqualToPercentile(int percentile) {
        return relationalToPercentile(percentile, gte);
    }

    public Supplier<List<TimeSeriesPoint>> relationalToPercentile(int percentile, RelationalOperator operator) {
        return () -> {
            ResponseEntity<String> response = restTemplate.exchange(
                    "/{owner}/{seriesName}/{distance}?percentile={percentile}&measurementId={measurementId}&operator={operator}",
                    HttpMethod.GET,
                    null,
                    String.class,
                    owner,
                    series,
                    distance,
                    percentile,
                    measurementId,
                    operator
            );
            assertEquals(200, response.getStatusCodeValue());
            try {
                return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public Supplier<List<TimeSeriesPoint>> lastInMonth() {
        return () -> {
            ResponseEntity<String> response = restTemplate.exchange(
                    "/{owner}/{seriesName}/minutes/last/months?from={from}&to={to}",
                    HttpMethod.GET,
                    null,
                    String.class,
                    owner,
                    series,
                    formatTimestamp(from),
                    formatTimestamp(to)
            );
            assertEquals(200, response.getStatusCodeValue());
            try {
                return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public Supplier<List<TimeSeriesPoint>> go() {
        return () -> {
            ResponseEntity<String> response = restTemplate.exchange(
                    "/{owner}/{seriesName}/{distance}",
                    HttpMethod.GET,
                    null,
                    String.class,
                    owner,
                    series,
                    distance
            );
            assertEquals(200, response.getStatusCodeValue());
            try {
                return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

}
