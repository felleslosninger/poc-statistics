package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.elasticsearch.IdResolver;
import no.difi.statistics.ingest.api.IngestResponse;
import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.ingest.elasticsearch.config.ElasticsearchConfig;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.test.utils.ElasticsearchHelper;
import no.difi.statistics.test.utils.ElasticsearchRule;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
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
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Collections.singletonList;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.ingest.api.IngestResponse.Status.Conflict;
import static no.difi.statistics.ingest.api.IngestResponse.Status.Ok;
import static no.difi.statistics.model.TimeSeriesDefinition.builder;
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
@TestPropertySource(properties = {"file.base.difi-statistikk=src/test/resources/apikey"})
@RunWith(SpringRunner.class)
public class ElasticsearchIngestServiceTest {

    @ClassRule
    public static ElasticsearchRule elasticsearchRule = new ElasticsearchRule();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.elasticsearch.host=" + elasticsearchRule.getHost(),
                    "no.difi.statistics.elasticsearch.port=" + elasticsearchRule.getPort()
            );
        }

    }

    private final ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 0, 0, 0, 0, ZoneOffset.UTC);

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
    public void prepare() {
        authenticationService = MockRestServiceServer.bindTo(authenticationRestTemplate).build();
        authenticationService
                .expect(manyTimes(), requestTo("http://authenticate:8080/authentications"))
                .andExpect(method(POST))
                .andExpect(content().contentType(APPLICATION_JSON_UTF8))
                .andExpect(jsonPath("username", equalTo(owner)))
                .andExpect(jsonPath("password", equalTo(password)))
                .andRespond(withSuccess("{\"authenticated\": true}", APPLICATION_JSON_UTF8));
        elasticsearchHelper = new ElasticsearchHelper(client);
        elasticsearchHelper.waitForGreenStatus();
    }

    @After
    public void cleanup() {
        elasticsearchHelper.clear();
    }

    @Test
    public void whenBulkIngestingPointsThenAllPointsAreIngested() {
        List<TimeSeriesPoint> points = newArrayList(
            point().timestamp(now).measurement("aMeasurement", 10546L).build(),
            point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 346346L).build(),
            point().timestamp(now.plusMinutes(2)).measurement("aMeasurement", 786543L).build()
        );
        TimeSeriesDefinition seriesDefinition = seriesDefinition().name("series").minutes().owner(owner);
        ResponseEntity<IngestResponse> response = ingest(seriesDefinition, points.get(0), points.get(1), points.get(2));
        assertEquals(3, response.getBody().getStatuses().size());
        for (IngestResponse.Status status : response.getBody().getStatuses())
            assertEquals(Ok, status);
        assertIngested(seriesDefinition, points, response.getBody());
    }

    @Test
    public void whenBulkIngestingDuplicatePointsThenAllPointsButDuplicatesAreIngested() {
        TimeSeriesPoint point1 = point().timestamp(now).measurement("aMeasurement", 103L).build();
        TimeSeriesPoint duplicateOfPoint1 = point().timestamp(now).measurement("aMeasurement", 2354L).build();
        TimeSeriesPoint point2 = point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 567543L).build();
        TimeSeriesDefinition seriesDefinition = seriesDefinition().name("series").minutes().owner(owner);
        ResponseEntity<IngestResponse> response = ingest(seriesDefinition, point1, duplicateOfPoint1, point2);
        assertEquals(3, response.getBody().getStatuses().size());
        assertIngested(seriesDefinition, 0, point1, response.getBody());
        assertNotIngested(1, response.getBody());
        assertIngested(seriesDefinition, 2, point2, response.getBody());
    }

    @Test
    public void whenIngestingDuplicatePointThenFailAndPointIsNotIngested() {
        TimeSeriesPoint point1 = point().timestamp(now).measurement("aMeasurement", 103L).build();
        TimeSeriesPoint duplicateOfPoint1 = point().timestamp(now).measurement("aMeasurement", 2354L).build();
        TimeSeriesPoint point2 = point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 567543L).build();
        TimeSeriesDefinition seriesDefinition = seriesDefinition().name("series").minutes().owner(owner);
        ResponseEntity<IngestResponse> response1 = ingest(seriesDefinition, point1);
        ResponseEntity<IngestResponse> response2 = ingest(seriesDefinition, duplicateOfPoint1);
        ResponseEntity<IngestResponse> response3 = ingest(seriesDefinition, point2);
        assertEquals(Ok, response1.getBody().getStatuses().get(0));
        assertEquals(Conflict, response2.getBody().getStatuses().get(0));
        assertEquals(Ok, response3.getBody().getStatuses().get(0));
        assertIngested(seriesDefinition, point1);
        assertIngested(seriesDefinition, point2);
    }

    @Test
    public void whenIngestingTwoPointsWithSameTimestampAndDifferentCategoriesThenBothAreIngested() {
        TimeSeriesPoint point = point().timestamp(now).category("category1", "abc").category("category2", "def").measurement("aMeasurement", 103L).build();
        TimeSeriesPoint pointWithDifferentCategory = point().timestamp(now).category("category1", "abc").measurement("aMeasurement", 2354L).build();
        TimeSeriesDefinition seriesDefinition = seriesDefinition().name("series").minutes().owner(owner);
        ResponseEntity<IngestResponse> response1 = ingest(seriesDefinition, point);
        ResponseEntity<IngestResponse> response2 = ingest(seriesDefinition, pointWithDifferentCategory);
        assertIngested(seriesDefinition, 0, point, response1.getBody());
        assertIngested(seriesDefinition, 0, pointWithDifferentCategory, response2.getBody());
    }

    @Test
    public void whenIngestingTwoPointsWithSameTimestampAndSameCategoriesThenLastPointIsNotIngested() {
        TimeSeriesPoint point1 = point().timestamp(now).category("category", "abc").measurement("aMeasurement", 103L).build();
        TimeSeriesPoint duplicateOfPoint1 = point().timestamp(now).category("category", "abc").measurement("aMeasurement", 2354L).build();
        TimeSeriesDefinition seriesDefinition = seriesDefinition().name("series").minutes().owner(owner);
        ResponseEntity<IngestResponse> response1 = ingest(seriesDefinition, point1);
        ResponseEntity<IngestResponse> response2 = ingest(seriesDefinition, duplicateOfPoint1);
        assertIngested(seriesDefinition, 0, point1, response1.getBody());
        assertNotIngested(0, response2.getBody());
    }

    @Test
    public void whenIngestingAPointThenProperlyNamedIndexIsCreated() {
        TimeSeriesDefinition seriesDefinition = seriesDefinition().name("series").minutes().owner(owner);
        ResponseEntity<IngestResponse> response =
                ingest(seriesDefinition, password, point().timestamp(now).measurement("aMeasurement", 103L).build());
        assertEquals(Ok, response.getBody().getStatuses().get(0));
        assertEquals(
                format("%s@%s@minute%d", owner, seriesDefinition.getName(), now.getYear()),
                elasticsearchHelper.indices()[0]
        );
    }

    @Test
    public void whenIngestingDuplicateOnTheHourPointThenFailAndPointIsNotIngested() {
        int addMinute = now.getMinute() == 59 ? -1 : 1;
        TimeSeriesPoint point1 = point().timestamp(now).measurement("aMeasurement", 105L).build();
        TimeSeriesPoint pointSameHourAsFirst = point().timestamp(now.plusMinutes(addMinute)).measurement("aMeasurement", 108L).build();
        TimeSeriesPoint pointNextHour = point().timestamp(now.plusHours(1)).measurement("aMeasurement", 115L).build();
        TimeSeriesDefinition seriesDefinition = seriesDefinition().name("series").hours().owner(owner);
        ResponseEntity<IngestResponse> response1 = ingest(seriesDefinition, point1);
        ResponseEntity<IngestResponse> response3 = ingest(seriesDefinition, pointSameHourAsFirst);
        ResponseEntity<IngestResponse> response4 = ingest(seriesDefinition, pointNextHour);
        assertEquals(Ok, response1.getBody().getStatuses().get(0));
        assertEquals(Conflict, response3.getBody().getStatuses().get(0));
        assertEquals(Ok, response4.getBody().getStatuses().get(0));
        assertIngestedHour(seriesDefinition, point1);
        assertIngestedHour(seriesDefinition, pointNextHour);
    }

    @Test
    public void givenASeriesWhenRequestingLastPointThenLastPointIsReturned() throws JSONException {
        List<TimeSeriesPoint> points = newArrayList(
                point().timestamp(now).measurement("aMeasurement", 10546L).build(),
                point().timestamp(now.plusMinutes(1)).measurement("aMeasurement", 346346L).build(),
                point().timestamp(now.plusMinutes(2)).measurement("aMeasurement", 786543L).build()
        );
        TimeSeriesDefinition seriesDefinition = seriesDefinition().name("series").minutes().owner(owner);
        ingest(seriesDefinition, points.get(0), points.get(1), points.get(2));
        elasticsearchHelper.refresh();
        JSONObject lastPoint = new JSONObject(last("series").getBody());
        assertEquals(now.plusMinutes(2).format(ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")), lastPoint.get("timestamp"));
    }

    @Test
    public void givenNoSeriesWhenRequestingLastPointThenNothingIsReturned() {
        assertNull(last("series").getBody());
    }

    private void assertIngested(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> points, IngestResponse response) {
        elasticsearchHelper.refresh();
        for (int i = 0; i < points.size(); i++)
            assertIngested(seriesDefinition, i, points.get(i), response);
        assertEquals(points.size(), elasticsearchHelper.search(singletonList("*"), now.minusDays(1), now.plusDays(1)).getHits().getTotalHits().value);
    }

    private void assertNotIngested(int index, IngestResponse response) {
        assertEquals(Conflict, response.getStatuses().get(index));
    }

    private void assertIngested(TimeSeriesDefinition seriesDefinition, int index, TimeSeriesPoint point, IngestResponse response) {
        assertEquals(Ok, response.getStatuses().get(index));
        assertIngested(seriesDefinition, point);
    }

    private void assertIngested(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint point) {
        String id = IdResolver.id(point, seriesDefinition);
        assertEquals(
                point.getMeasurement("aMeasurement").get(),
                elasticsearchHelper.get(
                        resolveIndexName()
                                .seriesDefinition(builder().name("series").minutes().owner(owner))
                                .at(point.getTimestamp())
                                .single(),
                        id,
                        "aMeasurement"
                )
        );
    }

    private void assertIngestedHour(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint point) {
        String id = IdResolver.id(point, seriesDefinition);
        assertEquals(
                point.getMeasurement("aMeasurement").get(),
                elasticsearchHelper.get(
                        resolveIndexName()
                                .seriesDefinition(builder().name("series").hours().owner(owner))
                                .at(point.getTimestamp())
                                .single(),
                        id,
                        "aMeasurement"
                )
        );
    }

    private TimeSeriesPoint.Builder point() {
        return TimeSeriesPoint.builder();
    }

    private TimeSeriesDefinition.NameEntry seriesDefinition() {
        return TimeSeriesDefinition.builder();
    }

    private ResponseEntity<IngestResponse> ingest(TimeSeriesDefinition seriesDefinition, String password, TimeSeriesPoint...points) {
        return restTemplate.postForEntity(
                "/{owner}/{seriesName}/{distance}",
                request(points, seriesDefinition.getOwner(), password),
                IngestResponse.class,
                seriesDefinition.getOwner(),
                seriesDefinition.getName(),
                seriesDefinition.getDistance()
        );
    }

    private ResponseEntity<IngestResponse> ingest(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint...points) {
        return ingest(seriesDefinition, password, points);
    }

    private ResponseEntity<String> last(String series) {
        return restTemplate.getForEntity(
                "/{owner}/{seriesName}/minutes/last",
                String.class,
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
