package no.difi.statistics;

import no.difi.statistics.model.RelationalOperator;
import no.difi.statistics.model.TimeSeriesPoint;

import java.util.List;
import java.util.function.Function;

import static no.difi.statistics.model.RelationalOperator.gt;
import static no.difi.statistics.model.RelationalOperator.gte;
import static no.difi.statistics.model.RelationalOperator.lt;
import static no.difi.statistics.model.RelationalOperator.lte;
import static no.difi.statistics.test.utils.DataOperations.relativeToPercentile;

public class PercentileFilterBuilder implements Function<List<TimeSeriesPoint>, List<TimeSeriesPoint>> {

    private RelationalOperator operator;
    private int percentile;
    private String measurementId;

    private PercentileFilterBuilder(RelationalOperator operator) {
        this.operator = operator;
    }

    static PercentileFilterBuilder pointsLessThanPercentile(int percentile) {
        return new PercentileFilterBuilder(lt).percentile(percentile);
    }

    static PercentileFilterBuilder pointsGreaterThanPercentile(int percentile) {
        return new PercentileFilterBuilder(gt).percentile(percentile);
    }

    static PercentileFilterBuilder pointsLessThanOrEqualToPercentile(int percentile) {
        return new PercentileFilterBuilder(lte).percentile(percentile);
    }

    static PercentileFilterBuilder pointsGreaterThanOrEqualToPercentile(int percentile) {
        return new PercentileFilterBuilder(gte).percentile(percentile);
    }

    private PercentileFilterBuilder percentile(int percentile) {
        this.percentile = percentile;
        return this;
    }

    PercentileFilterBuilder forMeasurement(String measurementId) {
        this.measurementId = measurementId;
        return this;
    }

    @Override
    public List<TimeSeriesPoint> apply(List<TimeSeriesPoint> points) {
        return relativeToPercentile(operator, measurementId, percentile).apply(points);
    }

}
