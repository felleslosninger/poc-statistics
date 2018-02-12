package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.MeasurementDistance;

public class TimeSeriesHistogramQuery extends TimeSeriesQuery {

    private MeasurementDistance targetDistance;

    public TimeSeriesHistogramQuery per(MeasurementDistance targetDistance) {
        this.targetDistance = targetDistance;
        return this;
    }

    public MeasurementDistance targetDistance() {
        return targetDistance;
    }

}
