package no.difi.statistics;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.test.utils.ElasticsearchHelper;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.iterate;
import static no.difi.statistics.test.utils.DataOperations.unit;

public class GivenSupplier implements Supplier<List<TimeSeriesPoint>> {

    private static final Random random = new Random();

    private String owner = "test_owner"; // Index names must be lower case in Elasticsearch
    private String series = "test";
    private List<String> measurementIds = asList("m1", "m2", "m3", "m4");
    private MeasurementDistance distance;
    private ZonedDateTime from = null;
    private ZonedDateTime to = null;
    private Long size = null;
    private List<TimeSeriesPoint> points;
    private ElasticsearchHelper helper;

    public GivenSupplier(ElasticsearchHelper helper) {
        this.helper = helper;
    }

    public GivenSupplier withName(String name) {
        this.series = name;
        return this;
    }

    public GivenSupplier withDistance(MeasurementDistance distance) {
        this.distance = distance;
        return this;
    }

    public GivenSupplier from(ZonedDateTime from) {
        this.from = from;
        return this;
    }

    public GivenSupplier to(ZonedDateTime to) {
        this.to = to;
        return this;
    }

    public GivenSupplier withMeasurements(String...measurements) {
        this.measurementIds = asList(measurements);
        return this;
    }

    public String id() {
        return distance.toString();
    }

    @Override
    public List<TimeSeriesPoint> get() {
        if (this.points != null)
            return this.points; // Use cache
        supply();
        return this.points;
    }

    private List<TimeSeriesPoint> supply() {
        if (from != null && to != null && size == null) {
            size = unit(distance).between(from, to);
        } else if (from == null && to != null && size != null) {
            from = to.minus(size, unit(distance));
        } else if (from != null && to == null && size != null) {
            // We're good
        } else if (from == null && to == null && size == null) {
            from = ZonedDateTime.of(1977, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
            size = 100L;
        } else {
            throw new IllegalArgumentException("Invalid time bounds specified");
        }
        this.points = iterate(from, from -> from.plus(1, unit(distance)))
                .map(t -> TimeSeriesPoint.builder().timestamp(t).measurements(randomMeasurements(measurementIds)).build())
                .limit(size)
                .collect(toList());
        try {
            helper.indexPoints(this.owner, this.series, this.distance, this.points);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return points;
    }

    private static List<Measurement> randomMeasurements(List<String> ids) {
        return ids.stream().map(id -> new Measurement(id, random.nextInt(1_000_000))).collect(toList());
    }
}
