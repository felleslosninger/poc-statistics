package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.TimeSeriesPoint;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;

public class ExecutedLastQuery extends ExecutedTimeSeriesQuery {

    public ExecutedLastQuery(TimeSeriesQuery query) {
        super(query);
    }

    @Override
    protected List<TimeSeriesPoint> deserialize(String response) throws IOException {
        return singletonList(objectMapper.readerFor(TimeSeriesPoint.class).readValue(response));
    }

    @Override
    protected String url() {
        return "/{owner}/{series}/{distance}/last" + queryUrl();
    }

}
