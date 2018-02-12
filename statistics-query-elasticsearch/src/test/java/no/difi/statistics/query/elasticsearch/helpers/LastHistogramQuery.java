package no.difi.statistics.query.elasticsearch.helpers;

import static no.difi.statistics.test.utils.DataOperations.lastPer;

public class LastHistogramQuery extends TimeSeriesHistogramQuery {

    public static LastHistogramQuery calculatedLastHistogram() {
        LastHistogramQuery query = new LastHistogramQuery();
        query.function(verifier(query));
        return query;
    }

    public static LastHistogramQuery requestingLastHistogram() {
        LastHistogramQuery query = new LastHistogramQuery();
        query.function(executor(query));
        return query;
    }

    @Override
    public LastHistogramQuery toCalculated() {
        LastHistogramQuery query = new LastHistogramQuery();
        query
                .per(targetDistance())
                .owner(owner())
                .name(name())
                .from(from())
                .to(to())
                .distance(distance())
                .categories(categories())
                .function(verifier(query));
        return query;
    }

    private static TimeSeriesFunction executor(TimeSeriesHistogramQuery query) {
        return givenSeries -> new ExecutedLastHistogramQuery(query).per(query.targetDistance()).execute();
    }

    private static TimeSeriesFunction verifier(TimeSeriesHistogramQuery query) {
        return givenSeries -> lastPer(query.selectFrom(givenSeries), query.categories(), query.targetDistance());
    }

}
