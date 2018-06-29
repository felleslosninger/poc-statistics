package no.difi.statistics.query.elasticsearch.helpers;

import static java.util.Collections.singletonList;
import static no.difi.statistics.test.utils.TimeSeriesSumCollector.summarize;

public class SumQuery extends TimeSeriesQuery {

    public static SumQuery requestingSum() {
        return new SumQuery(false);
    }

    public static SumQuery calculatedSum() {
        return new SumQuery(true);
    }

    private SumQuery(boolean calculated) {
        super();
        if (calculated)
            function(verifier());
        else
            function(executor());
    }

    @Override
    public SumQuery toCalculated() {
        SumQuery query = new SumQuery(true);
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
        return givenSeries -> QueryClient.execute("/{owner}/{series}/{distance}/sum" + queryUrl(), parameters(), true);
    }

    private TimeSeriesFunction verifier() {
        return givenSeries -> singletonList(selectFrom(givenSeries).getPoints().stream()
                .filter(this::withinRange)
                .filter(point -> point.hasCategories(categories()))
                .collect(summarize(categories())));
    }

}
