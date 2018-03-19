package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.TimeSeriesPoint;

import java.util.Comparator;

import static java.util.Collections.singletonList;
import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.groupingBy;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;

public class LastQuery extends TimeSeriesQuery {

    public static LastQuery requestingLast() {
        LastQuery query = new LastQuery();
        query.function(executor(query));
        return query;
    }

    public static LastQuery calculatedLast() {
        LastQuery query = new LastQuery();
        query.function(verifier(query));
        return query;
    }

    @Override
    public LastQuery toCalculated() {
        LastQuery query = new LastQuery();
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

    private static TimeSeriesFunction executor(TimeSeriesQuery query) {
        return givenSeries -> new ExecutedLastQuery(query).execute();
    }

    private static TimeSeriesFunction verifier(TimeSeriesQuery query) {
        return givenSeries -> singletonList(query.selectFrom(givenSeries).getPoints().stream()
                .filter(query::withinRange)
                .filter(point -> point.hasCategories(query.categories()))
                .collect(groupingBy(TimeSeriesPoint::getTimestamp, summarize(query.categories())))
                .values().stream().sorted(reverseOrder()).findFirst().orElse(null));
    }

}
