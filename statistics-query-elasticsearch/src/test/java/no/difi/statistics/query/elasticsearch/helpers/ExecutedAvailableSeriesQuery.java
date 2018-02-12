package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ExecutedAvailableSeriesQuery {

    public static TestRestTemplate restTemplate;
    public static ObjectMapper objectMapper;

    public List<TimeSeriesDefinition> execute() {
        ResponseEntity<String> response = restTemplate.exchange(
                "/meta",
                HttpMethod.GET,
                null,
                String.class
        );
        assertEquals(200, response.getStatusCodeValue());
        try {
            return objectMapper.readerFor(new TypeReference<List<TimeSeriesDefinition>>(){}).readValue(response.getBody());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
