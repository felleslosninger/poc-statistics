package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.ingest.config.AppConfig;
import no.difi.statistics.ingest.elasticsearch.config.ElasticsearchConfig;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
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
            GenericContainer backend = new GenericContainer("elasticsearch:2.3.5");
            backend.start();
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.elasticsearch.host=" + backend.getContainerIpAddress(),
                    "no.difi.statistics.elasticsearch.port=" + backend.getMappedPort(9300)
            );
            ElasticsearchIngestServiceTest.backend = backend;
        }

    }

    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 0, 0, 0, 0, ZoneId.of("UTC"));

    private static GenericContainer backend;

    @Autowired
    private Client client;
    @Autowired
    private TestRestTemplate restTemplate;

    @Before
    public void prepare() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            if (((TransportClient)client).connectedNodes().size() > 0) break;
            Thread.sleep(10L);
        }
    }

    @After
    public void cleanup() throws ExecutionException, InterruptedException {
        client.admin().indices().prepareDelete("_all").get();
    }

    @AfterClass
    public static void cleanupAll() {
        backend.stop();
    }

    @Test
    public void whenIngestingAPointThenProperlyNamedIndexIsCreated() {
        final String owner = "991825827";
        final String password = "654321";
        final String series = "series";
        ResponseEntity<Void> response = ingest(owner, password, series, TimeSeriesPoint.builder().timestamp(now).measurement("aMeasurement", 103L).build());
        assertEquals(200, response.getStatusCodeValue());
        assertEquals(format("%s:%s:minute%d.%02d.%02d", owner, series, now.getYear(), now.getMonthValue(), now.getDayOfMonth()), indices()[0]);
    }

    private String[] indices() {
        return client.admin().cluster()
                .prepareState().execute()
                .actionGet().getState()
                .getMetaData().concreteAllIndices();
    }

    private ResponseEntity<Void> ingest(String owner, String password, String series, TimeSeriesPoint point) {
        return restTemplate.postForEntity(
                "/minutes/{owner}/{seriesName}",
                request(point, owner, password),
                Void.class,
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
