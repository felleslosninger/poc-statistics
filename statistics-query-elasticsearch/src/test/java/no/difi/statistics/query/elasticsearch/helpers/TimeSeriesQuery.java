package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeries;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.boot.test.web.client.TestRestTemplate;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.sort;
import static java.util.stream.Collectors.groupingBy;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;

public class TimeSeriesQuery extends Query<List<TimeSeriesPoint>> {

    private String owner = "test_owner";
    private String name = "test";
    private ZonedDateTime from;
    private ZonedDateTime to;
    private MeasurementDistance distance;
    private Map<String, String> categories = new HashMap<>();

    public static TimeSeriesQuery requestingSeries() {
        TimeSeriesQuery query = new TimeSeriesQuery();
        query.function(executor(query));
        return query;
    }

    public static TimeSeriesQuery withAttributes() {
        return new TimeSeriesQuery();
    }

    private static TimeSeriesFunction verifier(TimeSeriesQuery query) {
        return givenSeries -> {
            List<TimeSeriesPoint> result = new ArrayList<>(
                    query.selectFrom(givenSeries).getPoints().stream()
                            .filter(query::withinRange)
                            .filter(point -> point.hasCategories(query.categories()))
                            .collect(groupingBy(TimeSeriesPoint::getTimestamp, summarize(query.categories()))).values()
            );
            sort(result);
            return result;
        };
    }

    private static TimeSeriesFunction executor(TimeSeriesQuery query) {
        return givenSeries -> new ExecutedTimeSeriesQuery(query).execute();
    }

    @Override
    public TimeSeriesQuery toCalculated() {
        TimeSeriesQuery query = new TimeSeriesQuery();
        query
                .owner(owner())
                .name(name())
                .from(from())
                .to(to())
                .distance(distance())
                .categories(categories())
                .function(verifier(query));
        return query;
    }

    public TimeSeriesQuery owner(String owner) {
        this.owner = owner;
        return this;
    }

    public TimeSeriesQuery name(String name) {
        this.name = name;
        return this;
    }

    public TimeSeriesQuery from(ZonedDateTime from) {
        this.from = from;
        return this;
    }

    public TimeSeriesQuery to(ZonedDateTime to) {
        this.to = to;
        return this;
    }

    public TimeSeriesQuery distance(MeasurementDistance distance) {
        this.distance = distance;
        return this;
    }

    public TimeSeriesQuery category(String key, String value) {
        categories.put(key, value);
        return this;
    }

    public TimeSeriesQuery categories(Map<String, String> categories) {
        this.categories.putAll(categories);
        return this;
    }

    public String owner() {
        return owner;
    }

    public String name() {
        return name;
    }

    public ZonedDateTime from() {
        return from;
    }

    public ZonedDateTime to() {
        return to;
    }

    public MeasurementDistance distance() {
        return distance;
    }

    public Map<String, String> categories() {
        return categories;
    }

    public TimeSeries selectFrom(List<TimeSeries> givenSeries) {
        if (distance != null)
            return givenSeries.stream().filter(s -> s.getDefinition().getDistance() == distance).findFirst().get();
        if (givenSeries.size() != 1)
            throw new IllegalArgumentException("Distance must be specified when number of given series is " + givenSeries.size());
        return givenSeries.get(0);
    }

    public boolean withinRange(TimeSeriesPoint point) {
        return !(from != null && point.getTimestamp().isBefore(from)) && !(to != null && point.getTimestamp().isAfter(to));
    }

}
