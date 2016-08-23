package no.difi.statistics.ingest.client.demo;

import no.difi.statistics.ingest.client.IngestClient;
import no.difi.statistics.ingest.client.IngestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.net.MalformedURLException;

@Configuration
@EnableAutoConfiguration
public class AppConfig {

    @Autowired
    private Environment environment;

    @Value("${serviceurl: http://eid-test-docker01.dmz.local:10009}")
    private String serviceURL;

    @Bean
    public IngestService service() throws MalformedURLException {
        return new IngestClient(serviceURL); }

    @Bean
    public IngestRestController api() throws MalformedURLException {
        return new IngestRestController(service());
    }
}
