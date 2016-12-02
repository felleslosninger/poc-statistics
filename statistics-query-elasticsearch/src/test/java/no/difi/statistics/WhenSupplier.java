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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;
import static no.difi.statistics.WhenSupplier.Type.*;
import static no.difi.statistics.model.RelationalOperator.*;

public class WhenSupplier implements Supplier<List<TimeSeriesPoint>> {

    enum Type {normal, relativeToPercentile, lastPer}

    private String owner = "test_owner"; // Index names must be lower case in Elasticsearch
    private String series = "test";
    private String measurementId;
    private MeasurementDistance distance;
    private MeasurementDistance targetDistance;
    private ZonedDateTime from;
    private ZonedDateTime to;
    private Integer percentile;
    private RelationalOperator operator;
    private Type type = normal;
    private TestRestTemplate restTemplate;
    private ObjectMapper objectMapper;
    private List<TimeSeriesPoint> points;
    private String failure;

    public WhenSupplier(TestRestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    public WhenSupplier from(String series) {
        this.series = series;
        return this;
    }

    public WhenSupplier from(ZonedDateTime from) {
        this.from = from;
        return this;
    }

    public WhenSupplier to(ZonedDateTime to) {
        this.to = to;
        return this;
    }

    public WhenSupplier withDistance(MeasurementDistance distance) {
        this.distance = distance;
        return this;
    }

    public WhenSupplier withMeasurement(String measurementId) {
        this.measurementId = measurementId;
        return this;
    }

    public WhenSupplier lessThanPercentile(int percentile) {
        return relationalToPercentile(percentile, lt);
    }

    public WhenSupplier greaterThanPercentile(int percentile) {
        return relationalToPercentile(percentile, gt);
    }

    public WhenSupplier lessThanOrEqualToPercentile(int percentile) {
        return relationalToPercentile(percentile, lte);
    }

    public WhenSupplier greaterThanOrEqualToPercentile(int percentile) {
        return relationalToPercentile(percentile, gte);
    }

    public WhenSupplier relationalToPercentile(int percentile, RelationalOperator operator) {
        this.percentile = percentile;
        this.operator = operator;
        this.type = relativeToPercentile;
        return this;
    }

    public WhenSupplier lastPer(MeasurementDistance targetDistance) {
        this.targetDistance = targetDistance;
        this.type = lastPer;
        return this;
    }

    @Override
    public synchronized List<TimeSeriesPoint> get() {
        if (points == null && failure == null)
            supply();
        if (failure != null)
            throw new RuntimeException("Supplier failed: " + failure);
        return points;
    }

    public String failure() {
        if (points == null && failure == null)
            supply();
        if (points != null)
            throw new RuntimeException("Wrong failure expectation for " + this);
        return failure;
    }

    private void supply() {
        ResponseEntity<String> response = restTemplate.exchange(
                url(),
                HttpMethod.GET,
                null,
                String.class,
                parameters()
        );
        if (response.getStatusCodeValue() != 200)
            failure = response.getStatusCodeValue() + (response.getBody() != null ? "/" + response.getBody() : "");
        else {
            try {
                this.points = objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String url() {
        switch (type) {
            case normal:
            case relativeToPercentile:
                return "/{owner}/{series}/{distance}" + queryUrl();
            case lastPer:
                return "/{owner}/{series}/{distance}/last/{targetDistance}" + queryUrl();
            default:
                throw new IllegalArgumentException(type.toString());
        }
    }

    private Map<String, Object> parameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("owner", owner);
        parameters.put("series", series);
        parameters.put("distance", distance);
        if (targetDistance != null) parameters.put("targetDistance", targetDistance);
        parameters.putAll(queryParameters());
        return parameters;
    }

    private Map<String, Object> queryParameters() {
        Map<String, Object> parameters = new HashMap<>();
        if (measurementId != null) parameters.put("measurementId", measurementId);
        if (from != null) parameters.put("from", formatTimestamp(from));
        if (to != null) parameters.put("to", formatTimestamp(to));
        if (percentile != null) parameters.put("percentile", percentile);
        if (operator != null) parameters.put("operator", operator);
        return parameters;
    }

    private String queryUrl() {
        String s = parameters().keySet().stream()
                .filter(p -> queryParameters().keySet().contains(p))
                .map(p -> p + "=" + "{" + p + "}")
                .collect(joining("&"));
        return s.isEmpty() ? s : "?" + s;
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

    @Override
    public String toString() {
        return "WhenSupplier{" +
                "owner='" + owner + '\'' +
                ", series='" + series + '\'' +
                ", measurementId='" + measurementId + '\'' +
                ", distance=" + distance +
                ", targetDistance=" + targetDistance +
                ", from=" + from +
                ", to=" + to +
                ", percentile=" + percentile +
                ", operator=" + operator +
                ", type=" + type +
                '}';
    }
}
