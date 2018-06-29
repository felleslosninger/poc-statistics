package no.difi.statistics.query.elasticsearch.helpers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class QueryClient {

    public static TestRestTemplate restTemplate;
    public static ObjectMapper objectMapper;

    public static List<TimeSeriesPoint> execute(String url, Map<String, Object> parameters, boolean singleResult) {
        String response = exchange(url, parameters);
        if (singleResult){
            return singletonList(readValue(response, new TypeReference<TimeSeriesPoint>(){}));
        } else {
            return readValue(response, new TypeReference<List<TimeSeriesPoint>>(){});
        }
    }

    public static <T> T execute(String url, Map<String, Object> parameters, TypeReference<T> resultType) {
        String response = exchange(url, parameters);
        return readValue(response, resultType);
    }

    private static <T> T readValue(String response, TypeReference<T> type) {
        try {
            return objectMapper.readerFor(type).readValue(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String exchange(String url, Map<String, Object> parameters) {
        ResponseEntity<String> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                String.class,
                parameters
        );
        if (response.getStatusCodeValue() != 200)
            throw new RuntimeException(response.getStatusCodeValue() + "/" + response.getBody());
        return response.getBody();
    }

}
