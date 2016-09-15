package no.difi.statistics.ingest.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.apache.tomcat.util.codec.binary.Base64;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.web.context.WebApplicationContext;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {AppConfig.class, MockBackendConfig.class})
@AutoConfigureMockMvc
public class IngestRestControllerTest {

    @Autowired
    private WebApplicationContext springContext;
    @Autowired
    private IngestService service;

    private static final String VALIDUSERNAME = "984661185";
    private static final String VALIDPASSWORD = "123456";

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void whenSendingRequestWithValidTimeSeriesPointAndValidLoginThenExpectValuesSentToServiceMethodToBeTheSameAsSentToService() throws Exception {
        ArgumentCaptor<TimeSeriesPoint> argumentCaptor = ArgumentCaptor.forClass(TimeSeriesPoint.class);
        String timeSeries = "test123";
        TimeSeriesPoint timeSeriesPoint = createValidTimeSeriesPoint(createValidMeasurements(), ZonedDateTime.now());
        postMinutes(timeSeries, createValidJsonString(timeSeriesPoint), VALIDUSERNAME, VALIDPASSWORD);

        verify(service).minute(
                eq(timeSeries),
                argumentCaptor.capture()
        );

        assertCorrectValues(timeSeriesPoint, argumentCaptor);
    }

    @Test
    public void whenSendingRequestWithValidTimeSeriesPointAndValidLoginThenExpectNormalResponse() throws Exception {
        String jsonString = createValidJsonString(createValidTimeSeriesPoint(createValidMeasurements(), ZonedDateTime.now()));
        ResultActions result = postMinutes("test", jsonString, VALIDUSERNAME, VALIDPASSWORD);
        assertNormalResponse(result);
    }

    @Test
    public void whenSendingRequestWithInvalidJsonAndValidLoginThenExpect400Response() throws Exception {
        ResultActions result = postMinutes("test", "invalidJson", VALIDUSERNAME, VALIDPASSWORD);
        assert400Response(result);
    }

    @Test
    public void whenSendingRequestWithInvalidLoginThenExpect401Response() throws Exception {
        String jsonString = createValidJsonString(createValidTimeSeriesPoint(createValidMeasurements(), ZonedDateTime.now()));
        ResultActions result = postMinutes("test", jsonString, VALIDUSERNAME, "123");
        assert401Response(result);
    }

    private void assert401Response(ResultActions result) throws Exception {
        result.andExpect(status().is(401));
    }

    private void assert400Response(ResultActions result) throws Exception {
        result.andExpect(status().is(400));
    }

    private void assertNormalResponse(ResultActions result) throws Exception {
        result.andExpect(status().is(200));
    }

    private TimeSeriesPoint createValidTimeSeriesPoint(List<Measurement> measurements, ZonedDateTime timestamp) {

        return TimeSeriesPoint.builder()
                .measurements(measurements)
                .timestamp(timestamp)
                .build();
    }

    private void assertCorrectValues(TimeSeriesPoint timeSeriesPoint, ArgumentCaptor<TimeSeriesPoint> argumentCaptor) {
        assertEquals(timeSeriesPoint.getTimestamp().withZoneSameInstant(ZoneId.of("UTC")),
                argumentCaptor.getValue().getTimestamp());
        assertEquals(timeSeriesPoint.getMeasurements().get(0).getId(),
                argumentCaptor.getValue().getMeasurements().get(0).getId());
        assertEquals(timeSeriesPoint.getMeasurements().get(0).getValue(),
                argumentCaptor.getValue().getMeasurements().get(0).getValue());
    }

    private List<Measurement> createValidMeasurements() {
        List<Measurement> measurements = new ArrayList<>();
        measurements.add(new Measurement("antall", 2));
        return measurements;
    }

    private String createBasicAuthHeaderValue(String username, String password) {
        return "Basic " + new String(Base64.encodeBase64((username + ":" + password).getBytes()));
    }

    private ResultActions postMinutes(String seriesName, String jsonString, String username, String password) throws Exception {
        String basicDigestHeaderValue = createBasicAuthHeaderValue(username, password);
        String typeJson = "application/json";
        return mockMvc.perform(post("/minutes/{seriesName}", seriesName)
                .contentType(typeJson)
                .accept(typeJson)
                .header("Authorization", basicDigestHeaderValue)
                .content(jsonString));
    }

    private String createValidJsonString(TimeSeriesPoint timeSeriesPoint) throws Exception {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .writeValueAsString(timeSeriesPoint);
    }

}
