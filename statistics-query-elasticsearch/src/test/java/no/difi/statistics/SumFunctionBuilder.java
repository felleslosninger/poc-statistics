package no.difi.statistics;

import no.difi.statistics.model.TimeSeriesPoint;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;

public class SumFunctionBuilder implements Function<List<TimeSeriesPoint>, List<TimeSeriesPoint>> {

    private ZonedDateTime from;
    private ZonedDateTime to;

    public static SumFunctionBuilder sumOfPoints() {
        return new SumFunctionBuilder();
    }

    public SumFunctionBuilder from(ZonedDateTime from) {
        this.from = from;
        return this;
    }

    public SumFunctionBuilder to(ZonedDateTime to) {
        this.to = to;
        return this;
    }

    @Override
    public List<TimeSeriesPoint> apply(List<TimeSeriesPoint> points) {
        return singletonList(points.stream().filter(this::withinRange).collect(summarize()));
    }

    private boolean withinRange(TimeSeriesPoint point) {
        return !(from != null && point.getTimestamp().isBefore(from)) && !(to != null && point.getTimestamp().isAfter(to));
    }

}
