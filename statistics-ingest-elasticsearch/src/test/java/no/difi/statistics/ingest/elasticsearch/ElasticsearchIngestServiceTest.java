package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.ingest.api.IngestResponse;
import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.ingest.elasticsearch.config.ElasticsearchConfig;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.test.utils.ElasticsearchHelper;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Collections.singletonList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.ingest.api.IngestResponse.Status.Failed;
import static no.difi.statistics.ingest.api.IngestResponse.Status.Ok;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.test.web.client.ExpectedCount.manyTimes;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

/**
 * Komponent- og (delvis) integrasjonstest av inndata-tjenesten. Integrasjon mot <code>elasticsearch</code>-tjenesten
 * verifiseres, mens <code>authenticate</code>-tjenesten mockes.
 */
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
    private static GenericContainer backend;

    @Autowired
    private TestRestTemplate restTemplate;
    @Autowired
    private Client client;
    private ElasticsearchHelper elasticsearchHelper;
    @Autowired
    private RestTemplate authenticationRestTemplate;
    private MockRestServiceServer authenticationService;
    private String owner = "123456789";
    private String password = "aPassword";

    @Before
    public void prepare() throws Exception {
        authenticationService = MockRestServiceServer.bindTo(authenticationRestTemplate).build();
        authenticationService
                .expect(manyTimes(), requestTo("http://authenticate:8080/authentications"))
                .andExpect(method(POST))
                .andExpect(content().contentType(APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("username", equalTo(owner)))
                .andExpect(jsonPath("password", equalTo(password)))
                .andRespond(withSuccess("{\"authenticated\": true}", APPLICATION_JSON_UTF8));
        elasticsearchHelper = new ElasticsearchHelper(
                client,
                backend.getContainerIpAddress(),
                backend.getMappedPort(9200)
        );
        elasticsearchHelper.waitForGreenStatus();
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
        List<TimeSeriesPoint> points = newArrayList(
            point().timestamp(now).measurement("aMeasurement", 10546L).build(),
            point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 346346L).build(),
            point().timestamp(now.plusMinutes(2)).measurement("aMeasurement", 786543L).build()
        );
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
    public void whenIngestingDuplicatePointThenFailAndPointIsNotIngested() throws InterruptedException, IOException {
        TimeSeriesPoint point1 = point().timestamp(now).measurement("aMeasurement", 103L).build();
        TimeSeriesPoint duplicateOfPoint1 = point().timestamp(now).measurement("aMeasurement", 2354L).build();
        TimeSeriesPoint point2 = point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 567543L).build();
        ResponseEntity<Void> response1 = ingest("minutes", "series", point1);
        ResponseEntity<Void> response2 = ingest("minutes", "series", duplicateOfPoint1);
        ResponseEntity<Void> response3 = ingest("minutes", "series", point2);
        assertEquals(200, response1.getStatusCodeValue());
        assertEquals(409, response2.getStatusCodeValue());
        assertEquals(200, response3.getStatusCodeValue());
        assertIngested(point1);
        assertIngested(point2);
    }

    @Test
    public void whenIngestingAPointThenProperlyNamedIndexIsCreated() {
        final String series = "series";
        ResponseEntity<Void> response =
                ingest("minutes", owner, password, series, point().timestamp(now).measurement("aMeasurement", 103L).build());
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(
                format("%s:%s:minute%d.%02d.%02d", owner, series, now.getYear(), now.getMonthValue(), now.getDayOfMonth()),
                elasticsearchHelper.indices()[0]
        );
    }

    @Test
    public void whenIngestingDuplicateOnTheHourPointThenFailAndPointIsNotIngested() throws InterruptedException, IOException {
        int addMinute = now.getMinute() == 59 ? -1 : 1;
        TimeSeriesPoint point1 = point().timestamp(now).measurement("aMeasurement", 105L).build();
        TimeSeriesPoint pointSameHourAsFirst = point().timestamp(now.plusMinutes(addMinute)).measurement("aMeasurement", 108L).build();
        TimeSeriesPoint pointNextHour = point().timestamp(now.plusHours(1)).measurement("aMeasurement", 115L).build();
        ResponseEntity<Void> response1 = ingest("hours", "series", point1);
        ResponseEntity<Void> response3 = ingest("hours", "series", pointSameHourAsFirst);
        ResponseEntity<Void> response4 = ingest("hours", "series", pointNextHour);
        assertEquals(HttpStatus.OK.value(), response1.getStatusCodeValue());
        assertEquals(HttpStatus.CONFLICT.value(), response3.getStatusCodeValue());
        assertEquals(HttpStatus.OK.value(), response4.getStatusCodeValue());
        assertIngestedHour(point1);
        assertIngestedHour(pointNextHour);
    }

    @Test
    public void givenASeriesWhenRequestingLastPointThenLastPointIsReturned() {
        List<TimeSeriesPoint> points = newArrayList(
                point().timestamp(now).measurement("aMeasurement", 10546L).build(),
                point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 346346L).build(),
                point().timestamp(now.plusMinutes(2)).measurement("aMeasurement", 786543L).build()
        );
        ingest("series", points.get(0), points.get(1), points.get(2));
        elasticsearchHelper.refresh();
        TimeSeriesPoint lastPoint = last("series").getBody();
        assertEquals(now.plusMinutes(2), lastPoint.getTimestamp());
    }

    @Test
    public void givenNoSeriesWhenRequestingLastPointThenNothingIsReturned() {
        assertNull(last("series").getBody());
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
        assertEquals(Ok, response.getStatuses().get(index));
        assertIngested(point);
    }

    private void assertIngested(TimeSeriesPoint point) {
        String id = documentId(point.getTimestamp());
        assertEquals(
                (Long)point.getMeasurement("aMeasurement").get().getValue(),
                elasticsearchHelper.get(
                        resolveIndexName().seriesName("series").owner(owner).minutes().at(point.getTimestamp()).single(),
                        id,
                        "aMeasurement"
                )
        );
    }

    private void assertIngestedHour(TimeSeriesPoint point) {
        String id = documentId(point.getTimestamp());
        assertEquals(
                (Long)point.getMeasurement("aMeasurement").get().getValue(),
                elasticsearchHelper.get(
                        resolveIndexName().seriesName("series").owner(owner).hours().at(point.getTimestamp()).single(),
                        id,
                        "aMeasurement"
                )
        );
    }

    private TimeSeriesPoint.Builder point() {
        return TimeSeriesPoint.builder();
    }

    private ResponseEntity<Void> ingest(String uriPart, String series, TimeSeriesPoint point) {
        return ingest(uriPart, owner, password, series, point);
    }

    private ResponseEntity<Void> ingest(String distance, String owner, String password, String series, TimeSeriesPoint point) {
        return restTemplate.postForEntity(
                "/{owner}/{seriesName}/{distance}",
                request(point, owner, password),
                Void.class,
                owner,
                series,
                distance
        );
    }

    private ResponseEntity<IngestResponse> ingest(String series, TimeSeriesPoint...points) {
        return restTemplate.postForEntity(
                "/{owner}/{seriesName}/minutes?bulk",
                request(points, owner, password),
                IngestResponse.class,
                owner,
                series
        );
    }

    private ResponseEntity<TimeSeriesPoint> last(String series) {
        return restTemplate.getForEntity(
                "/{owner}/{seriesName}/minutes/last",
                TimeSeriesPoint.class,
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

    private static String documentId(ZonedDateTime timestamp) {
        return normalize(timestamp).toString();
    }

    private static ZonedDateTime normalize(ZonedDateTime timestamp) {
        return timestamp.truncatedTo(MINUTES).withZoneSameInstant(UTC);
    }
}
