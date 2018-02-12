package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.TimeSeries;
import no.difi.statistics.model.TimeSeriesPoint;

import java.util.List;
import java.util.function.Function;

public interface TimeSeriesFunction extends Function<List<TimeSeries>, List<TimeSeriesPoint>> {
}
