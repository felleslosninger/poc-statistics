package no.difi.statistics.query.elasticsearch.helpers;

import static no.difi.statistics.test.utils.DataOperations.sumPer;

public class SumHistogramQuery extends TimeSeriesHistogramQuery {

    public static SumHistogramQuery calculatedSumHistogram() {
        SumHistogramQuery query = new SumHistogramQuery();
        query.function(verifier(query));
        return query;
    }

    public static SumHistogramQuery requestingSumHistogram() {
        SumHistogramQuery query = new SumHistogramQuery();
        query.function(executor(query));
        return query;
    }

    @Override
    public SumHistogramQuery toCalculated() {
        SumHistogramQuery query = new SumHistogramQuery();
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
        return givenSeries -> new ExecutedSumHistogramQuery(query)
                .per(query.targetDistance()).execute();
    }

    private static TimeSeriesFunction verifier(TimeSeriesHistogramQuery query) {
        return givenSeries -> sumPer(query.selectFrom(givenSeries), query.categories(), query.targetDistance());
    }

}
