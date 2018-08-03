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
    private String categoryKey;

    public static TimeSeriesSumCollector summarize(
            Function<ZonedDateTime, ZonedDateTime> timestampModifier,
            Map<String, String> targetCategories
    ) {
        return new TimeSeriesSumCollector(timestampModifier, targetCategories);
    }

    public static TimeSeriesSumCollector summarize(Map<String, String> targetCategories) {
        return new TimeSeriesSumCollector(null, targetCategories);
    }

    public static TimeSeriesSumCollector summarize(Map<String, String> targetCategories, String categoryKey) {
        return new TimeSeriesSumCollector(null, targetCategories, categoryKey);
    }

    public static TimeSeriesSumCollector summarize() {
        return new TimeSeriesSumCollector(null, null);
    }

    private TimeSeriesSumCollector(Function<ZonedDateTime, ZonedDateTime> timestampModifier, Map<String, String> targetCategories) {
        this(timestampModifier, targetCategories, null);
    }

    private TimeSeriesSumCollector(Function<ZonedDateTime, ZonedDateTime> timestampModifier, Map<String, String> targetCategories, String categoryKey) {
        this.timestampModifier = timestampModifier;
        this.targetCategories = targetCategories;
        this.categoryKey = categoryKey;
    }

    @Override
    public Supplier<TimeSeriesPoint.Builder> supplier() {
        return () -> {
            TimeSeriesPoint.Builder builder = TimeSeriesPoint.builder();
            if (timestampModifier != null)
                builder.timestampModifier(timestampModifier);
            if (targetCategories != null)
                builder.categories(targetCategories);
            return builder;
        };
    }

    @Override
    public BiConsumer<TimeSeriesPoint.Builder, TimeSeriesPoint> accumulator() {
        return (builder, point) -> {
            builder.add(point);
            if (categoryKey != null && point.hasCategory(categoryKey))
                builder.category(categoryKey, point.getCategoryValue(categoryKey));
        };
    }

    @Override
    public BinaryOperator<TimeSeriesPoint.Builder> combiner() {
        return (left, right) -> left.add(right.build());
    }

    @Override
    public Function<TimeSeriesPoint.Builder, TimeSeriesPoint> finisher() {
        return TimeSeriesPoint.Builder::build;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.singleton(Characteristics.UNORDERED);
    }
}
