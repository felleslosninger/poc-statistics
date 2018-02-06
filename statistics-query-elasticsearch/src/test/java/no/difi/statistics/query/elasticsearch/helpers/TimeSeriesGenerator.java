package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.*;
import no.difi.statistics.test.utils.ElasticsearchHelper;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.iterate;
import static no.difi.statistics.test.utils.DataOperations.unit;

public class TimeSeriesGenerator implements Supplier<TimeSeries> {

    private static final Random random = new Random();

    private String owner = "test_owner"; // Index names must be lower case in Elasticsearch
    private String seriesName = "test";
    private MeasurementDistance distance;
    private List<String> measurementIds = asList("m1", "m2", "m3", "m4");
    private List<String> categories = emptyList();
    private ZonedDateTime from = null;
    private ZonedDateTime to = null;
    private Long size = null;
    private TimeSeries series;
    private ElasticsearchHelper helper;

    public TimeSeriesGenerator(ElasticsearchHelper helper) {
        this.helper = helper;
    }

    public TimeSeriesGenerator withName(String name) {
        this.seriesName = name;
        return this;
    }

    public TimeSeriesGenerator withDistance(MeasurementDistance distance) {
        this.distance = distance;
        return this;
    }

    public TimeSeriesGenerator withOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public TimeSeriesGenerator withSize(long size) {
        this.size = size;
        return this;
    }

    public TimeSeriesGenerator from(ZonedDateTime from) {
        this.from = from;
        return this;
    }

    public TimeSeriesGenerator to(ZonedDateTime to) {
        this.to = to;
        return this;
    }

    public TimeSeriesGenerator withMeasurements(String...measurements) {
        this.measurementIds = asList(measurements);
        return this;
    }

    public TimeSeriesGenerator withCategories(String...categories) {
        this.categories = asList(categories);
        return this;
    }

    public String id() {
        return toString();
    }

    @Override
    public TimeSeries get() {
        if (this.series != null)
            return this.series; // Use cache
        supply();
        return this.series;
    }

    private void supply() {
        if (from != null && to != null && size == null) {
            size = unit(distance).between(from, to);
        } else if (from == null && to != null && size != null) {
            from = to.minus(size, unit(distance));
        } else if (from != null && to == null && size != null) {
            // We're good
        } else if (from != null && to == null) {
            size = 100L;
        } else if (from == null && to == null && size == null) {
            from = ZonedDateTime.of(1977, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
            size = 100L;
        } else if (from == null && to == null) {
            from = ZonedDateTime.of(1977, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
        } else {
            throw new IllegalArgumentException("Invalid time bounds specified");
        }
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(this.seriesName).distance(this.distance).owner(this.owner);
        this.series = new TimeSeries(
                seriesDefinition,
                iterate(from, from -> from.plus(1, unit(distance)))
                        .map(this::categorizedPoints)
                        .limit(size)
                        .flatMap(Collection::stream)
                        .collect(toList())
        );
        helper.indexPoints(seriesDefinition, this.series.getPoints());
    }

    private List<TimeSeriesPoint> categorizedPoints(ZonedDateTime timestamp) {
        if (categories.isEmpty())
            return singletonList(aPoint(timestamp).build());
        return categories.stream().map(category -> aPoint(timestamp, category).build()).collect(toList());
    }

    private TimeSeriesPoint.Builder aPoint(ZonedDateTime timestamp, String category) {
        return aPoint(timestamp).category(category.split("=")[0], category.split("=")[1]);
    }

    private TimeSeriesPoint.Builder aPoint(ZonedDateTime timestamp) {
        return TimeSeriesPoint.builder()
                .timestamp(timestamp)
                .measurements(randomMeasurements(measurementIds));
    }

    private Map<String, String> randomCategories() {
        List<String> categories = new ArrayList<>();
        categories.addAll(this.categories);
        return IntStream.range(0, random.nextInt(categories.size() + 1))
                .mapToObj(i -> categories.remove(random.nextInt(categories.size())))
                .collect(toMap(category -> category.split("=")[0], category -> category.split("=")[1], (a, b) -> b));
    }

    private static List<Measurement> randomMeasurements(List<String> ids) {
        return ids.stream().map(id -> new Measurement(id, random.nextInt(1_000_000))).collect(toList());
    }

    @Override
    public String toString() {
        return format("%s:%s:%s", owner, seriesName, distance);
    }
}
