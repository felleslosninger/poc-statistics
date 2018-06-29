package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.core.type.TypeReference;
import no.difi.statistics.model.TimeSeries;
import no.difi.statistics.model.TimeSeriesDefinition;

import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class AvailableSeriesQuery extends Query<List<TimeSeriesDefinition>> {

    public static AvailableSeriesQuery calculatedAvailableSeries() {
        AvailableSeriesQuery query = new AvailableSeriesQuery();
        query.function(
            givenSeries -> givenSeries.stream().map(TimeSeries::getDefinition).sorted().collect(toList())
        );
        return query;
    }

    public static AvailableSeriesQuery requestingAvailableTimeSeries() {
        AvailableSeriesQuery query = new AvailableSeriesQuery();
        query.function(
                givenSeries -> QueryClient.execute("/meta", Collections.emptyMap(), new TypeReference<List<TimeSeriesDefinition>>(){})
        );
        return query;
    }

    @Override
    public <T> T toCalculated() {
        return null; // TODO
    }

}
