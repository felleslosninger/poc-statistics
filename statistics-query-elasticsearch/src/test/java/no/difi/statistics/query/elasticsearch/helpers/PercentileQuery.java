package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.RelationalOperator;

import static no.difi.statistics.model.RelationalOperator.*;
import static no.difi.statistics.test.utils.DataOperations.relativeToPercentile;

public class PercentileQuery extends TimeSeriesQuery {

    private RelationalOperator operator;
    private int percentile;
    private String measurementId;

    public static PercentileQuery calculatedPercentile() {
        PercentileQuery query = new PercentileQuery();
        query.function(verifier(query));
        return query;
    }

    public static PercentileQuery requestingPercentile() {
        PercentileQuery query = new PercentileQuery();
        query.function(executor(query));
        return query;
    }

    @Override
    public PercentileQuery toCalculated() {
        PercentileQuery query = new PercentileQuery();
        query
                .operator(operator)
                .percentile(percentile)
                .withMeasurement(measurementId)
                .owner(owner())
                .name(name())
                .from(from())
                .to(to())
                .distance(distance())
                .categories(categories())
                .function(verifier(query));
        return query;
    }

    public static TimeSeriesFunction executor(PercentileQuery query) {
        return givenSeries -> new ExecutedPercentileQuery(query).execute();
    }

    private static TimeSeriesFunction verifier(PercentileQuery query) {
        return givenSeries -> relativeToPercentile(query.operator(), query.measurementId(), query.percentile())
                .apply(query.selectFrom(givenSeries));
    }

    public PercentileQuery withMeasurement(String measurementId) {
        this.measurementId = measurementId;
        return this;
    }

    private PercentileQuery operator(RelationalOperator operator) {
        this.operator = operator;
        return this;
    }

    private PercentileQuery percentile(int percentile) {
        this.percentile = percentile;
        return this;
    }

    public PercentileQuery lessThan(int percentile) {
        operator(lt);
        percentile(percentile);
        return this;
    }

    public PercentileQuery greaterThan(int percentile) {
        this.operator = gt;
        this.percentile = percentile;
        return this;
    }

    public PercentileQuery lessThanOrEqualTo(int percentile) {
        this.operator = lte;
        this.percentile = percentile;
        return this;
    }

    public PercentileQuery greaterThanOrEqualTo(int percentile) {
        this.operator = gte;
        this.percentile = percentile;
        return this;
    }

    public RelationalOperator operator() {
        return operator;
    }

    public int percentile() {
        return percentile;
    }

    public String measurementId() {
        return measurementId;
    }

}
