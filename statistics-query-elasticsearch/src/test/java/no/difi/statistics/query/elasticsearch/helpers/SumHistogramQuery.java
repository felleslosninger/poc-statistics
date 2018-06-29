package no.difi.statistics.query.elasticsearch.helpers;

import static no.difi.statistics.test.utils.DataOperations.sumPer;

public class SumHistogramQuery extends TimeSeriesHistogramQuery {

    public static SumHistogramQuery calculatedSumHistogram() {
        return new SumHistogramQuery(true);
    }

    public static SumHistogramQuery requestingSumHistogram() {
        return new SumHistogramQuery(false);
    }

    private SumHistogramQuery(boolean calculated) {
        if (calculated)
            function(verifier());
        else
            function(executor());
    }

    @Override
    public SumHistogramQuery toCalculated() {
        SumHistogramQuery query = new SumHistogramQuery(true);
        query
                .per(targetDistance())
                .owner(owner())
                .name(name())
                .from(from())
                .to(to())
                .distance(distance())
                .categories(categories());
        return query;
    }

    private TimeSeriesFunction executor() {
        return givenSeries -> QueryClient.execute("/{owner}/{series}/{distance}/sum/" + targetDistance() + queryUrl(), parameters(), false);
    }

    private TimeSeriesFunction verifier() {
        return givenSeries -> sumPer(selectFrom(givenSeries), categories(), targetDistance());
    }

}
