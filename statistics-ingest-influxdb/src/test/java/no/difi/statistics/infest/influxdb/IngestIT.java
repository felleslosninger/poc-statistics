package no.difi.statistics.infest.influxdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.test.utils.DockerHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.time.ZonedDateTime;

import static io.restassured.RestAssured.given;
import static java.lang.String.format;

public class IngestIT {

    private String apiBaseUrl;
    final static DockerHelper dockerHelper = new DockerHelper();
    private static ObjectMapper objectMapper;

    @BeforeClass
    public static void initAbstractAll() throws UnknownHostException {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Before
    public void init() {
        apiBaseUrl = format(
                "http://%s:%s",
                dockerHelper.address(),
                dockerHelper.portFor(8080, apiContainerName())
        );
    }

    protected String apiContainerName() {
        return "/statistics-ingest-influxdb";
    }

    @Test
    public void givenAThenB() throws JsonProcessingException {
        TimeSeriesPoint point = TimeSeriesPoint.builder()
                .timestamp(ZonedDateTime.now())
                .measurement("test", 103)
                .build();
        given().contentType("application/json").body(objectMapper.writeValueAsString(point))
                .when().post(apiBaseUrl + "/minutes/test")
                .then().assertThat().statusCode(200);
    }

}
