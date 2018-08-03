package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeries;
import no.difi.statistics.model.TimeSeriesPoint;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;

import static java.util.Collections.sort;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;

public class TimeSeriesQuery extends Query<List<TimeSeriesPoint>> {

    private String owner = "test_owner";
    private String name = "test";
    private ZonedDateTime from;
    private ZonedDateTime to;
    private MeasurementDistance distance;
    private Map<String, String> categories = new HashMap<>();
    private String perCategory;

    public static TimeSeriesQuery requestingSeries() {
        return new TimeSeriesQuery(false);
    }

    // Used for generating (given)
    public static TimeSeriesQuery withAttributes() {
        return new TimeSeriesQuery();
    }

    private TimeSeriesQuery(boolean calculated) {
        if (calculated)
            function(verifier());
        else
            function(executor());
    }

    TimeSeriesQuery() {
    }

    private TimeSeriesFunction verifier() {
        return givenSeries -> {
            List<TimeSeriesPoint> result = new ArrayList<>(
                    selectFrom(givenSeries).getPoints().stream()
                            .filter(this::withinRange)
                            .filter(point -> point.hasCategories(categories()))
                            .filter(point -> perCategory() == null || point.hasCategory(perCategory()))
                            .collect(groupingBy(groupingClassifier(), summarize(categories(), perCategory()))).values()
            );
            sort(result);
            return result;
        };
    }

    private Function<TimeSeriesPoint, String> groupingClassifier() {
        return (point) -> point.getTimestamp() + "-" + point.getCategories().map(c -> c.get(perCategory())).orElse("");
    }

    private TimeSeriesFunction executor() {
        return givenSeries -> QueryClient.execute("/{owner}/{series}/{distance}" + queryUrl(), parameters(), false);
    }

    @Override
    public TimeSeriesQuery toCalculated() {
        TimeSeriesQuery query = new TimeSeriesQuery(true);
        query
                .owner(owner())
                .name(name())
                .from(from())
                .to(to())
                .distance(distance())
                .categories(categories())
                .perCategory(perCategory());
        return query;
    }

    protected Map<String, Object> parameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("owner", owner());
        parameters.put("series", name());
        parameters.put("distance", distance());
        parameters.putAll(queryParameters());
        return parameters;
    }

    protected Map<String, Object> queryParameters() {
        Map<String, Object> parameters = new HashMap<>();
        if (from() != null) parameters.put("from", formatTimestamp(from()));
        if (to() != null) parameters.put("to", formatTimestamp(to()));
        if (perCategory() != null) parameters.put("perCategory", perCategory());
        if (categories() != null && !categories().isEmpty())
            parameters.put("categories", categories().entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(joining(",")));
        return parameters;
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

    protected String queryUrl() {
        String s = queryParameters().keySet().stream()
                .map(p -> p + "=" + "{" + p + "}")
                .collect(joining("&"));
        return s.isEmpty() ? s : "?" + s;
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

    public TimeSeriesQuery perCategory(String perCategory){
        this.perCategory = perCategory;
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

    public String perCategory() {
        return perCategory;
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
