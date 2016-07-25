package no.difi.statistics.query.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.config.BackendConfig;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.mockito.Mockito.verify;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests the REST API in isolation with Spring Mock MVC as driver and a Mockito-stubbed service.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {AppConfig.class, MockBackendConfig.class})
@WebAppConfiguration
public class QueryRestControllerTest {

    @Autowired
    private WebApplicationContext springContext;
    @Autowired
    private BackendConfig backendConfig;
    private MockMvc mockMvc;

    @Before
    public void before(){
        mockMvc = MockMvcBuilders.webAppContextSetup(springContext).build();
    }

    @Test
    public void whenSendingRequestWithFilterThenServiceReceivesCorrespondingRequest() throws Exception {
        final String timeSeries = "a_series";
        final String from = "2013-10-12T12:13:13.123+02:00";
        final String to = "2013-10-12T13:13:13.123+02:00";
        TimeSeriesFilter filter = new TimeSeriesFilter(3, "anId");
        ResultActions result;
        result = mockMvc.perform(
                post("/minutes/{series}/total", timeSeries)
                        .param("from", from)
                        .param("to", to)
                        .content(json(filter))
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
        );
        assertNormalResponse(result);
        verify(backendConfig.queryService()).minutes(
                timeSeries,
                "total",
                parseTimestamp(from),
                parseTimestamp(to),
                filter
        );
    }

    @Test
    @Ignore
    public void whenSendingRequestWithoutFromAndToThenExpectNormalResponseAndNoRangeInServiceCall() throws Exception {
        final String timeSeries = "test";
        ResultActions result = mockMvc.perform(get("/minutes/{series}/total", timeSeries));
        assertNormalResponse(result);
        verify(backendConfig.queryService()).minutes(
                timeSeries,
                "total",
                null,
                null
        );
    }

    @Test
    @Ignore
    public void whenSendingRequestWithoutFromThenExpectNormalResponseAndLeftOpenRangeInServiceCall() throws Exception {
        final String endTime = "2013-10-12T13:13:13.123+02:00";
        final String timeSeries = "test";
        ResultActions result = mockMvc.perform(get("/minutes/{series}/total", timeSeries).param("to", endTime));
        assertNormalResponse(result);
        verify(backendConfig.queryService()).minutes(
                timeSeries,
                "total",
                null,
                parseTimestamp(endTime)
        );
    }

    @Test
    @Ignore
    public void whenSendingRequestWithoutToThenExpectNormalResponseAndRightOpenRangeInServiceCall() throws Exception {
        final String startTime = "2013-10-12T13:13:13.123+02:00";
        final String timeSeries = "test";
        ResultActions result = mockMvc.perform(get("/minutes/{series}/total", timeSeries).param("from", startTime));
        assertNormalResponse(result);
        verify(backendConfig.queryService()).minutes(
                timeSeries,
                "total",
                parseTimestamp(startTime),
                null
        );
    }

    private ZonedDateTime parseTimestamp(String timestamp) {
        return ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    private byte[] json(Object o) throws JsonProcessingException {
        return new ObjectMapper().writeValueAsBytes(o);
    }

    private void assertNormalResponse(ResultActions result) throws Exception {
        result.andExpect(status().is(200)).andExpect(content().contentType(APPLICATION_JSON_UTF8));
    }

}
