package no.difi.statistics;

import no.difi.statistics.config.AppConfig;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.System.out;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = AppConfig.class)
public class StatisticsIT {

    @Autowired
    private Client client;
    @Autowired
    private Statistics statistics;

    private String indexName = "test-index";
    private ZonedDateTime now = ZonedDateTime.now();

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        client.admin().indices().delete(new DeleteIndexRequest(indexName)).get();
    }

    @Test
    public void givenTimeSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(1000), 1000);
        indexTimeSeriesMinutePoint(now.minusMinutes(1001), 1001);
        indexTimeSeriesMinutePoint(now.minusMinutes(1002), 1002);
        indexTimeSeriesMinutePoint(now.minusMinutes(1003), 1003);
        List<TimeSeriesPoint> timeSeries = statistics.minutes(
                indexName,
                now.minusMinutes(1002),
                now.minusMinutes(1001)
        );
        assertEquals(2, timeSeries.size());
        assertEquals(1002, timeSeries.get(0).value());
        assertEquals(1001, timeSeries.get(1).value());
    }

    @Test
    public void givenTimeSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(20), 20);
        indexTimeSeriesMinutePoint(now.minusMinutes(19), 19);
        indexTimeSeriesMinutePoint(now.minusMinutes(18), 18);
        indexTimeSeriesMinutePoint(now.minusMinutes(17), 17);
        List<TimeSeriesPoint> timeSeries = statistics.minutes(
                indexName,
                now.minusMinutes(9),
                now.minusMinutes(8)
        );
        assertEquals(0, timeSeries.size());
    }

    @Test
    public void givenTimeSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException, InterruptedException {
        indexTimeSeriesMinutePoint(now.minusMinutes(100), 100);
        indexTimeSeriesMinutePoint(now.minusMinutes(200), 200);
        indexTimeSeriesMinutePoint(now.minusMinutes(300), 300);
        indexTimeSeriesMinutePoint(now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = statistics.minutes(
                indexName,
                now.minusYears(2_000),
                now.plusYears(2_000)
        );
        timeSeries.stream().forEach(t -> out.println(t.time() + ": " + t.value()));
        assertEquals(4, timeSeries.size());
    }

    private void indexTimeSeriesMinutePoint(ZonedDateTime timestamp, int value) throws IOException {
        client.prepareIndex(indexName, "minutes")
                .setSource(
                        jsonBuilder().startObject()
                                .field("time", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp))
                                .field("value", value)
                                .endObject()
                )
                .setRefresh(true) // Make document immediately searchable for the purpose of this test
                .get();
    }

}
