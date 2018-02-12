package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.test.web.client.TestRestTemplate;

public class ExecutedSumHistogramQuery extends ExecutedHistogramQuery {

    ExecutedSumHistogramQuery(TimeSeriesQuery filter) {
        super(filter);
    }

    @Override
    protected String url() {
        return "/{owner}/{series}/{distance}/sum/" + targetDistance + queryUrl();
    }

}
