package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeries;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.test.utils.ElasticsearchHelper;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.iterate;
import static no.difi.statistics.test.utils.DataOperations.unit;

public class TimeSeriesGenerator implements Supplier<TimeSeries> {

    private static final Random random = new Random();

    private List<String> measurementIds = asList("m1", "m2", "m3", "m4");
    private TimeSeriesQuery attributes;
    private Long size = null;
    private long defaultSize = 100L;
    private TimeSeries series;
    private ElasticsearchHelper helper;

    public TimeSeriesGenerator(ElasticsearchHelper helper) {
        this.helper = helper;
    }

    public TimeSeriesGenerator withAttributes(TimeSeriesQuery attributes) {
        this.attributes = attributes;
        return this;
    }

    public TimeSeriesGenerator withSize(long size) {
        this.size = size;
        return this;
    }

    public TimeSeriesGenerator withMeasurements(String...measurements) {
        this.measurementIds = asList(measurements);
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
        if (attributes.from() != null && attributes.to() != null && size == null) {
            size = unit(attributes.distance()).between(attributes.from(), attributes.to());
        } else if (attributes.from() == null && attributes.to() != null && size != null) {
            attributes.from(attributes.to().minus(size, unit(attributes.distance())));
        } else if (attributes.from() != null && attributes.to() == null && size != null) {
            // We're good
        } else if (attributes.from() != null && attributes.to() == null) {
            size = defaultSize;
        } else if (attributes.from() == null && attributes.to() == null && size == null) {
            attributes.from(ZonedDateTime.of(1977, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")));
            size = defaultSize;
        } else if (attributes.from() == null && attributes.to() == null) {
            attributes.from(ZonedDateTime.of(1977, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")));
        } else {
            throw new IllegalArgumentException("Invalid time bounds specified");
        }
        TimeSeriesDefinition seriesDefinition = TimeSeriesDefinition.builder().name(attributes.name()).distance(attributes.distance()).owner(attributes.owner());
        this.series = new TimeSeries(
                seriesDefinition,
                iterate(attributes.from(), from -> from.plus(1, unit(attributes.distance())))
                        .map(this::categorizedPoints)
                        .limit(size)
                        .flatMap(Collection::stream)
                        .collect(toList())
        );
        helper.indexPoints(seriesDefinition, this.series.getPoints());
    }

    private List<TimeSeriesPoint> categorizedPoints(ZonedDateTime timestamp) {
        if (attributes.categories().isEmpty())
            return singletonList(aPoint(timestamp).build());
        return attributes.categories().entrySet().stream().map(category -> aPoint(timestamp, category).build()).collect(toList());
    }

    private TimeSeriesPoint.Builder aPoint(ZonedDateTime timestamp, Map.Entry<String, String> category) {
        return aPoint(timestamp).category(category.getKey(), category.getValue());
    }

    private TimeSeriesPoint.Builder aPoint(ZonedDateTime timestamp) {
        return TimeSeriesPoint.builder()
                .timestamp(timestamp)
                .measurements(randomMeasurements(measurementIds));
    }

    private static List<Measurement> randomMeasurements(List<String> ids) {
        return ids.stream().map(id -> new Measurement(id, random.nextInt(1_000_000))).collect(toList());
    }

    @Override
    public String toString() {
        return format("%s:%s:%s", attributes.owner(), attributes.name(), attributes.distance());
    }
}
