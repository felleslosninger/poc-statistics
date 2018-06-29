package no.difi.statistics.query.elasticsearch.helpers;

import static no.difi.statistics.test.utils.DataOperations.lastPer;

public class LastHistogramQuery extends TimeSeriesHistogramQuery {

    public static LastHistogramQuery calculatedLastHistogram() {
        return new LastHistogramQuery(true);
    }

    public static LastHistogramQuery requestingLastHistogram() {
        return new LastHistogramQuery(false);
    }

    private LastHistogramQuery(boolean calculated) {
        if (calculated)
            function(verifier());
        else
            function(executor());
    }

    @Override
    public LastHistogramQuery toCalculated() {
        LastHistogramQuery query = new LastHistogramQuery(true);
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
        return givenSeries -> QueryClient.execute("/{owner}/{series}/{distance}/last/" + targetDistance() + queryUrl(), parameters(), false);
    }

    private TimeSeriesFunction verifier() {
        return givenSeries -> lastPer(selectFrom(givenSeries), categories(), targetDistance());
    }

}
