package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.TimeSeriesPoint;

import static java.util.Collections.singletonList;
import static java.util.Comparator.reverseOrder;
import static java.util.stream.Collectors.groupingBy;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;

public class LastQuery extends TimeSeriesQuery {

    public static LastQuery requestingLast() {
        return new LastQuery(false);
    }

    public static LastQuery calculatedLast() {
        return new LastQuery(true);
    }

    private LastQuery(boolean calculated) {
        if (calculated)
            function(verifier());
        else
            function(executor());
    }

    @Override
    public LastQuery toCalculated() {
        LastQuery query = new LastQuery(true);
        query
                .owner(owner())
                .name(name())
                .from(from())
                .to(to())
                .distance(distance())
                .categories(categories());
        return query;
    }

    private TimeSeriesFunction executor() {
        return givenSeries -> QueryClient.execute("/{owner}/{series}/{distance}/last" + queryUrl(), parameters(), true);
    }

    private TimeSeriesFunction verifier() {
        return givenSeries -> singletonList(selectFrom(givenSeries).getPoints().stream()
                .filter(this::withinRange)
                .filter(point -> point.hasCategories(categories()))
                .collect(groupingBy(TimeSeriesPoint::getTimestamp, summarize(categories())))
                .values().stream().sorted(reverseOrder()).findFirst().orElse(null));
    }

}
