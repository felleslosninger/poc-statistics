package no.difi.statistics.ingest.elasticsearch.config;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.BackendConfig;
import no.difi.statistics.ingest.elasticsearch.ElasticsearchIngestService;
import no.difi.statistics.ingest.elasticsearch.ElasticsearchUserDetailsService;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.userdetails.UserDetailsService;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@EnableAutoConfiguration
public class ElasticsearchConfig implements BackendConfig {

    @Autowired
    private Environment environment;

    @Bean
    @Override
    public IngestService ingestService() {
        return new ElasticsearchIngestService(elasticsearchClient());
    }

    @Bean(destroyMethod = "close")
    public Client elasticsearchClient() {
        String host = environment.getRequiredProperty("no.difi.statistics.elasticsearch.host");
        int port = environment.getRequiredProperty("no.difi.statistics.elasticsearch.port", Integer.class);
        try {
            return TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to create Elasticsearch client", e);
        }
    }

    @Bean
    @Override
    public UserDetailsService userDetailsService() {
        return new ElasticsearchUserDetailsService();
    }

}
