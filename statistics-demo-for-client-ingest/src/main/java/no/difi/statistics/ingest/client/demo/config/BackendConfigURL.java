package no.difi.statistics.ingest.client.demo.config;


import no.difi.statistics.ingest.client.IngestClient;
import no.difi.statistics.ingest.client.IngestService;
import no.difi.statistics.ingest.client.demo.config.BackendConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.net.MalformedURLException;

@Configuration
public class BackendConfigURL implements BackendConfig {

    @Autowired
    private Environment environment;

    @Value("${serviceurl: http://eid-test-docker01.dmz.local:10009}")
    private String serviceURL;

    @Override
    @Bean
    public IngestService ingestService() throws MalformedURLException {
        return new IngestClient(serviceURL); }
}
