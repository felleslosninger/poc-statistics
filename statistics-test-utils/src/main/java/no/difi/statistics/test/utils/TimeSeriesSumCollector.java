package no.difi.statistics.test.utils;

import no.difi.statistics.model.TimeSeriesPoint;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class TimeSeriesSumCollector implements Collector<TimeSeriesPoint, TimeSeriesPoint.Builder, TimeSeriesPoint> {

    private Function<ZonedDateTime, ZonedDateTime> timestampModifier;
    private Map<String, String> targetCategories;

    public static TimeSeriesSumCollector summarize(
            Function<ZonedDateTime, ZonedDateTime> timestampModifier,
            Map<String, String> targetCategories
    ) {
        return new TimeSeriesSumCollector(timestampModifier, targetCategories);
    }

    public static TimeSeriesSumCollector summarize(Map<String, String> targetCategories) {
        return new TimeSeriesSumCollector(null, targetCategories);
    }

    public static TimeSeriesSumCollector summarize() {
        return new TimeSeriesSumCollector(null, null);
    }

    private TimeSeriesSumCollector(Function<ZonedDateTime, ZonedDateTime> timestampModifier, Map<String, String> targetCategories) {
        this.timestampModifier = timestampModifier;
        this.targetCategories = targetCategories;
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
            if (targetCategories != null)
                b.categories(targetCategories);
            return b.build();
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.singleton(Characteristics.UNORDERED);
    }
}
