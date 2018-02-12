package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.RelationalOperator;
import org.springframework.boot.test.web.client.TestRestTemplate;

import java.util.Map;

public class ExecutedPercentileQuery extends ExecutedTimeSeriesQuery {

    private Integer percentile;
    private RelationalOperator operator;
    private String measurementId;

    ExecutedPercentileQuery(PercentileQuery attributes) {
        super(attributes);
        this.percentile = attributes.percentile();
        this.operator = attributes.operator();
        this.measurementId = attributes.measurementId();
    }

    @Override
    protected Map<String, Object> queryParameters() {
        Map<String, Object> parameters = super.queryParameters();
        if (measurementId != null) parameters.put("measurementId", measurementId);
        if (percentile != null) parameters.put("percentile", percentile);
        if (operator != null) parameters.put("operator", operator);
        return parameters;
    }

    @Override
    protected String url() {
        return "/{owner}/{series}/{distance}/percentile" + queryUrl();
    }

}
