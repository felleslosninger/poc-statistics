package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.MeasurementDistance;
import org.springframework.boot.test.web.client.TestRestTemplate;

public class ExecutedHistogramQuery extends ExecutedTimeSeriesQuery {

    protected MeasurementDistance targetDistance;

    ExecutedHistogramQuery(TimeSeriesQuery filter) {
        super(filter);
    }

    public ExecutedHistogramQuery per(MeasurementDistance targetDistance) {
        this.targetDistance = targetDistance;
        return this;
    }

}
