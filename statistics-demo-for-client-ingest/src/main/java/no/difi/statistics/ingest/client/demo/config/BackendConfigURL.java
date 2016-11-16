package no.difi.statistics.ingest.client.demo.config;

import no.difi.statistics.ingest.client.IngestClient;
import no.difi.statistics.ingest.client.IngestService;
import no.difi.statistics.ingest.client.exception.MailformedUrl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class BackendConfigURL implements BackendConfig {

    @Autowired
    private Environment environment;

    @Override
    @Bean
    public IngestService ingestService() throws MailformedUrl {
        return new IngestClient(
                environment.getRequiredProperty("service.url"),
                environment.getRequiredProperty("owner"),
                environment.getRequiredProperty("user"),
                environment.getRequiredProperty("pwd"));
    }
}
