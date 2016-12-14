package no.difi.statistics.model;

import java.util.List;

public class TimeSeries {

    private TimeSeriesDefinition definition;
    private List<TimeSeriesPoint> points;

    public TimeSeries(TimeSeriesDefinition definition, List<TimeSeriesPoint> points) {
        this.definition = definition;
        this.points = points;
    }

    public TimeSeriesDefinition getDefinition() {
        return definition;
    }

    public List<TimeSeriesPoint> getPoints() {
        return points;
    }

}
