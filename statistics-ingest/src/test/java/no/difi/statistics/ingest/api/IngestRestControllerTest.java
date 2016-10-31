package no.difi.statistics.ingest.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.model.TimeSeriesPoint;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.apache.tomcat.util.codec.binary.Base64.encodeBase64;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {AppConfig.class, MockBackendConfig.class})
@AutoConfigureMockMvc
public class IngestRestControllerTest {

    @Autowired
    private IngestService service;

    private static final String VALIDUSERNAME = "984661185";
    private static final String VALIDPASSWORD = "123456";

    @Autowired
    private MockMvc mockMvc;

    @After
    public void resetMock() {
        reset(service);
    }

    @Test
    public void whenRequestingIndexThenAuthorizationIsNotRequired() throws Exception {
        mockMvc.perform(get("/")).andExpect(status().is(200));
    }

    @Test
    public void whenIngestingAndUserIsNotTheSameAsOwnerThenAccessIsDenied() throws Exception {
        mockMvc.perform(aRequestWithOwnerDifferentThanUser().content(json(aPoint())))
                .andExpect(status().is(403));
    }

    @Test
    public void whenSendingRequestWithValidTimeSeriesPointAndValidLoginThenExpectValuesSentToServiceMethodToBeTheSameAsSentToService() throws Exception {
        TimeSeriesPoint timeSeriesPoint = aPoint();
        mockMvc.perform(aRequest().content(json(timeSeriesPoint))).andExpect(status().is(200));
        verify(service).minute(eq("aTimeSeries"), eq(VALIDUSERNAME), eq(timeSeriesPoint));
    }

    @Test
    public void whenSendingValidRequestThenExpectNormalResponse() throws Exception {
        mockMvc.perform(aRequest().content(json(aPoint()))).andExpect(status().is(200));
    }

    @Test
    public void whenSendingRequestWithInvalidContentThenExpect400Response() throws Exception {
        mockMvc.perform(aRequest().content("invalidJson"))
                .andExpect(status().is(400));
    }

    @Test
    public void whenSendingRequestWithWrongPasswordThenExpect401Response() throws Exception {
        mockMvc.perform(aRequestWithWrongPassword().content(json(aPoint())))
                .andExpect(status().is(401));
    }

    private TimeSeriesPoint aPoint() {
        return TimeSeriesPoint.builder()
                .measurement("antall", 2)
                .timestamp(ZonedDateTime.of(2016, 3, 3, 20, 12, 13, 12, ZoneId.of("UTC")))
                .build();
    }

    private MockHttpServletRequestBuilder aRequest() {
        return baseRequest(VALIDUSERNAME, "aTimeSeries").header("Authorization", authorizationHeader(VALIDUSERNAME, VALIDPASSWORD));
    }

    private MockHttpServletRequestBuilder aRequestWithOwnerDifferentThanUser() {
        return baseRequest("ownerisnotuser", "aTimeSeries").header("Authorization", authorizationHeader(VALIDUSERNAME, VALIDPASSWORD));
    }

    private MockHttpServletRequestBuilder aRequestWithWrongPassword() {
        return baseRequest(VALIDUSERNAME, "aTimeSeries").header("Authorization", authorizationHeader(VALIDUSERNAME, "wrongPassword"));
    }

    private MockHttpServletRequestBuilder baseRequest(String owner, String seriesName) {
        return post("/{owner}/{seriesName}/minute", owner, seriesName).contentType(MediaType.APPLICATION_JSON_UTF8);
    }

    private String authorizationHeader(String username, String password) {
        return "Basic " + new String(encodeBase64((username + ":" + password).getBytes()));
    }

    private String json(TimeSeriesPoint timeSeriesPoint) throws Exception {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .writeValueAsString(timeSeriesPoint);
    }

}
