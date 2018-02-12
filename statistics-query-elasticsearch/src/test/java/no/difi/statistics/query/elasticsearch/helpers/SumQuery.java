package no.difi.statistics.query.elasticsearch.helpers;

import static java.util.Collections.singletonList;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;

public class SumQuery extends TimeSeriesQuery {

    public static SumQuery requestingSum() {
        SumQuery query = new SumQuery();
        query.function(executor(query));
        return query;
    }

    public static SumQuery calculatedSum() {
        SumQuery query = new SumQuery();
        query.function(verifier(query));
        return query;
    }

    @Override
    public SumQuery toCalculated() {
        SumQuery query = new SumQuery();
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
        return givenSeries -> new ExecutedSumQuery(query).execute();
    }

    private static TimeSeriesFunction verifier(TimeSeriesQuery query) {
        return givenSeries -> singletonList(query.selectFrom(givenSeries).getPoints()
                .stream().filter(query::withinRange).collect(summarize()));
    }

}
