package no.difi.statistics.ingest.client.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.client.demo.config.AppConfig;
import no.difi.statistics.ingest.client.model.Measurement;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.context.WebApplicationContext;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {AppConfig.class, MockBackendConfig.class})
@WebAppConfiguration
@AutoConfigureMockMvc
public class IngestRestControllerTest {

    @Autowired
    private WebApplicationContext springContext;
    @Autowired
    private MockMvc mockMvc;

    @Test
    public void whenSendingRequestWithValidTimeSeriesPointThenExpectNormalResponse() throws Exception {
        ResultActions result = postMinutes("test", createValidJsonString(createValidTimeSeriesPoint(createValidMeasurements(), ZonedDateTime.now())));
        assertNormalResponse(result);
    }

    @Test
    public void whenSendingRequestWithInvalidJsonThenExpect400Response() throws Exception {
        ResultActions result = postMinutes("test", "invalidJson");
        assert400Response(result);
    }

    @Test
    public void whenSendingRequestWithInvalidContentTypeThenExpect415Response() throws Exception {
        String jsonString = createValidJsonString(createValidTimeSeriesPoint(createValidMeasurements(), ZonedDateTime.now()));
        ResultActions result = postMinutesWithoutContentType("test", jsonString);

        assert415Response(result);
    }

    private void assert400Response(ResultActions result) throws Exception{
        result.andExpect(status().is(400));
    }

    private void assert415Response(ResultActions result) throws Exception{
        result.andExpect(status().is(415));
    }

    private void assertNormalResponse(ResultActions result) throws Exception {
        result.andExpect(status().is(200));
    }

    private TimeSeriesPoint createValidTimeSeriesPoint(List<Measurement> measurements, ZonedDateTime timestamp){

        return TimeSeriesPoint.builder()
                .measurements(measurements)
                .timestamp(timestamp)
                .build();
    }

    private List<Measurement> createValidMeasurements(){
        List<Measurement> measurements = new ArrayList<>();
        measurements.add(new Measurement("antall", 2));
        return measurements;
    }

    private ResultActions postMinutes(String seriesName, String jsonString) throws Exception {
        String typeJson = "application/json";
        return mockMvc.perform(post("/minutes/{seriesName}", seriesName)
                .contentType(typeJson)
                .accept(typeJson)
                .content(jsonString));
    }


    private ResultActions postMinutesWithoutContentType(String seriesName, String jsonString) throws Exception{
        String typeJson = "application/json";
        return mockMvc.perform(post("/minutes/{seriesName}", seriesName)
                .accept(typeJson)
                .content(jsonString));
    }

    private String createValidJsonString(TimeSeriesPoint timeSeriesPoint) throws Exception{
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .writeValueAsString(timeSeriesPoint);
    }

}
