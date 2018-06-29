package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.RelationalOperator;

import java.util.Map;

import static no.difi.statistics.model.RelationalOperator.*;
import static no.difi.statistics.test.utils.DataOperations.relativeToPercentile;

public class PercentileQuery extends TimeSeriesQuery {

    private RelationalOperator operator;
    private Integer percentile;
    private String measurementId;

    public static PercentileQuery calculatedPercentile() {
        return new PercentileQuery(true);
    }

    public static PercentileQuery requestingPercentile() {
        return new PercentileQuery(false);
    }

    private PercentileQuery(boolean calculated) {
        if (calculated)
            function(verifier());
        else
            function(executor());
    }

    @Override
    public PercentileQuery toCalculated() {
        PercentileQuery query = new PercentileQuery(true);
        query
                .operator(operator)
                .percentile(percentile)
                .withMeasurement(measurementId)
                .owner(owner())
                .name(name())
                .from(from())
                .to(to())
                .distance(distance())
                .categories(categories());
        return query;
    }

    private TimeSeriesFunction executor() {
        return givenSeries -> QueryClient.execute("/{owner}/{series}/{distance}/percentile/" + queryUrl(), parameters(), false);
    }

    @Override
    protected Map<String, Object> queryParameters() {
        Map<String, Object> parameters = super.queryParameters();
        if (measurementId != null) parameters.put("measurementId", measurementId);
        if (percentile != null) parameters.put("percentile", percentile);
        if (operator != null) parameters.put("operator", operator);
        return parameters;
    }

    private TimeSeriesFunction verifier() {
        return givenSeries -> relativeToPercentile(operator(), measurementId(), percentile())
                .apply(selectFrom(givenSeries));
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
