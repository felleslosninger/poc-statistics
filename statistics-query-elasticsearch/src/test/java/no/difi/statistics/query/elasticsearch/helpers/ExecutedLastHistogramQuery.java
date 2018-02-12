package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.test.web.client.TestRestTemplate;

public class ExecutedLastHistogramQuery extends ExecutedHistogramQuery {

    ExecutedLastHistogramQuery(TimeSeriesHistogramQuery query) {
        super(query);
    }

    @Override
    protected String url() {
        return "/{owner}/{series}/{distance}/last/" + targetDistance + queryUrl();
    }

}
