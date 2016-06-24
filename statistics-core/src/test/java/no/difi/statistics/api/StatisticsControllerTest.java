package no.difi.statistics.api;

import no.difi.statistics.Statistics;
import no.difi.statistics.config.AppConfig;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Tests the REST API in isolation with Spring Mock MVC as driver and a Mockito-stubbed service.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@WebAppConfiguration
public class StatisticsControllerTest {

    @Configuration
    public static class Config extends AppConfig {

        @Bean
        @Override
        public Statistics statistics() {
            return mock(Statistics.class);
        }

        @Bean
        @Override
        public Client elasticSearchClient() {
            return mock(Client.class);
        }

    }

    @Autowired
    private WebApplicationContext springContext;
    @Autowired
    private Statistics statistics;
    private MockMvc mockMvc;

    @Before
    public void before(){
        mockMvc = MockMvcBuilders.webAppContextSetup(springContext).build();
    }

    @Test
    public void whenSendingRequestWithoutFromAndToThenExpectNormalResponseAndNoRangeInServiceCall() throws Exception {
        final String timeSeries = "test";
        ResultActions result = mockMvc.perform(get("/minutes/{series}", timeSeries));
        assertNormalResponse(result);
        verify(statistics).minutes(
                timeSeries,
                null,
                null
        );
    }

    @Test
    public void whenSendingRequestWithoutFromThenExpectNormalResponseAndLeftOpenRangeInServiceCall() throws Exception {
        final String endTime = "2013-10-12T13:13:13.123+02:00";
        final String timeSeries = "test";
        ResultActions result = mockMvc.perform(get("/minutes/{series}", timeSeries).param("to", endTime));
        assertNormalResponse(result);
        verify(statistics).minutes(
                timeSeries,
                null,
                ZonedDateTime.parse(endTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        );
    }

    @Test
    public void whenSendingRequestWithoutToThenExpectNormalResponseAndRightOpenRangeInServiceCall() throws Exception {
        final String startTime = "2013-10-12T13:13:13.123+02:00";
        final String timeSeries = "test";
        ResultActions result = mockMvc.perform(get("/minutes/{series}", timeSeries).param("from", startTime));
        assertNormalResponse(result);
        verify(statistics).minutes(
                timeSeries,
                ZonedDateTime.parse(startTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                null
        );
    }

    private void assertNormalResponse(ResultActions result) throws Exception {
        result.andExpect(status().is(200)).andExpect(content().contentType(APPLICATION_JSON_UTF8));
    }

}
