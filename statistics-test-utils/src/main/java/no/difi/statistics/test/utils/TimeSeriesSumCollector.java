package no.difi.statistics.test.utils;

import no.difi.statistics.model.TimeSeriesPoint;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class TimeSeriesSumCollector implements Collector<TimeSeriesPoint, TimeSeriesPoint.Builder, TimeSeriesPoint> {

    private Function<ZonedDateTime, ZonedDateTime> timestampModifier;

    public static TimeSeriesSumCollector summarize(Function<ZonedDateTime, ZonedDateTime> timestampModifier) {
        return new TimeSeriesSumCollector(timestampModifier);
    }

    public static TimeSeriesSumCollector summarize() {
        return new TimeSeriesSumCollector(null);
    }

    private TimeSeriesSumCollector(Function<ZonedDateTime, ZonedDateTime> timestampModifier) {
        this.timestampModifier = timestampModifier;
    }

    @Override
    public Supplier<TimeSeriesPoint.Builder> supplier() {
        return TimeSeriesPoint::builder;
    }

    @Override
    public BiConsumer<TimeSeriesPoint.Builder, TimeSeriesPoint> accumulator() {
        return TimeSeriesPoint.Builder::add;
    }

    @Override
    public BinaryOperator<TimeSeriesPoint.Builder> combiner() {
        return (left, right) -> left.add(right.build());
    }

    @Override
    public Function<TimeSeriesPoint.Builder, TimeSeriesPoint> finisher() {
        return b -> {
            if (timestampModifier != null)
                b.timestampModifier(timestampModifier);
            return b.build();
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.singleton(Characteristics.UNORDERED);
    }
}
