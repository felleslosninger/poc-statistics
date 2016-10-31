package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.ingest.elasticsearch.config.ElasticsearchConfig;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.ingest.IngestResponse;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.model.ingest.IngestResponse.Status.Failed;
import static no.difi.statistics.model.ingest.IngestResponse.Status.Ok;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(
        webEnvironment = RANDOM_PORT
)
@ContextConfiguration(classes = {AppConfig.class, ElasticsearchConfig.class}, initializers = ElasticsearchIngestServiceTest.Initializer.class)
@RunWith(SpringRunner.class)
public class ElasticsearchIngestServiceTest {

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            GenericContainer backend = new GenericContainer("elasticsearch:2.4.1");
            backend.start();
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.elasticsearch.host=" + backend.getContainerIpAddress(),
                    "no.difi.statistics.elasticsearch.port=" + backend.getMappedPort(9300)
            );
            ElasticsearchIngestServiceTest.backend = backend;
        }

    }

    private final ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 0, 0, 0, 0, ZoneId.of("UTC"));
    private final String owner = "991825827";
    private static GenericContainer backend;

    @Autowired
    private TestRestTemplate restTemplate;
    @Autowired
    private Client client;
    private ElasticsearchHelper elasticsearchHelper;

    @Before
    public void prepare() throws InterruptedException, MalformedURLException, UnknownHostException {
        elasticsearchHelper = new ElasticsearchHelper(
                client,
                backend.getContainerIpAddress(),
                backend.getMappedPort(9200)
        );
        elasticsearchHelper.waitConnected();
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        elasticsearchHelper.clear();
    }

    @AfterClass
    public static void cleanupAll() {
        backend.stop();
    }

    @Test
    public void whenBulkIngestingPointsThenAllPointsAreIngested() throws InterruptedException, IOException {
        List<TimeSeriesPoint> points = new ArrayList<>();
        points.add(point().timestamp(now).measurement("aMeasurement", 10546L).build());
        points.add(point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 346346L).build());
        points.add(point().timestamp(now.plusMinutes(2)).measurement("aMeasurement", 786543L).build());
        ResponseEntity<IngestResponse> response = ingest("series", points.get(0), points.get(1), points.get(2));
        assertEquals(200, response.getStatusCodeValue());
        assertIngested(points, response.getBody());
    }

    @Test
    public void whenBulkIngestingDuplicatePointsThenAllPointsButDuplicatesAreIngested() throws InterruptedException, IOException {
        TimeSeriesPoint point1 = point().timestamp(now).measurement("aMeasurement", 103L).build();
        TimeSeriesPoint duplicateOfPoint1 = point().timestamp(now).measurement("aMeasurement", 2354L).build();
        TimeSeriesPoint point2 = point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 567543L).build();
        ResponseEntity<IngestResponse> response = ingest("series", point1, duplicateOfPoint1, point2);
        assertEquals(200, response.getStatusCodeValue());
        assertIngested(0, point1, response.getBody());
        assertNotIngested(1, response.getBody());
        assertIngested(2, point2, response.getBody());
    }

    @Test
    public void whenIngestingAPointThenProperlyNamedIndexIsCreated() {
        final String owner = "991825827";
        final String password = "654321";
        final String series = "series";
        ResponseEntity<Void> response = ingest(owner, password, series, point().timestamp(now).measurement("aMeasurement", 103L).build());
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(
                format("%s:%s:minute%d.%02d.%02d", owner, series, now.getYear(), now.getMonthValue(), now.getDayOfMonth()),
                elasticsearchHelper.indices()[0]
        );
    }

    private void assertIngested(List<TimeSeriesPoint> points, IngestResponse response) {
        elasticsearchHelper.refresh();
        for (int i = 0; i < points.size(); i++)
            assertIngested(i, points.get(i), response);
        assertEquals(points.size(), elasticsearchHelper.search(singletonList("*"), now.minusDays(1), now.plusDays(1)).getHits().totalHits());
    }

    private void assertNotIngested(int index, IngestResponse response) {
        assertEquals(Failed, response.getStatuses().get(index));
    }

    private void assertIngested(int index, TimeSeriesPoint point, IngestResponse response) {
        String id = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(point.getTimestamp());
        assertEquals(Ok, response.getStatuses().get(index));
        assertEquals(
                (Long)point.getMeasurement("aMeasurement").get().getValue(),
                elasticsearchHelper.get(
                        resolveIndexName().seriesName("series").owner(owner).minutes().at(point.getTimestamp()).single(),
                        id,
                        "aMeasurement"
                )
        );
    }

    private TimeSeriesPoint.Builder point() {
        return TimeSeriesPoint.builder();
    }

    private ResponseEntity<Void> ingest(String owner, String password, String series, TimeSeriesPoint point) {
        return restTemplate.postForEntity(
                "/{owner}/{seriesName}/minute",
                request(point, owner, password),
                Void.class,
                owner,
                series
        );
    }

    private ResponseEntity<IngestResponse> ingest(String series, TimeSeriesPoint...points) {
        final String password = "654321";
        return restTemplate.postForEntity(
                "/{owner}/{seriesName}/minutes",
                request(points, owner, password),
                IngestResponse.class,
                owner,
                series
        );
    }

    private <T> HttpEntity<T> request(T entity, String owner, String password) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Basic " + Base64.getEncoder().encodeToString(format("%s:%s", owner, password).getBytes()));
        return new HttpEntity<>(
                entity,
                headers
        );
    }

}
