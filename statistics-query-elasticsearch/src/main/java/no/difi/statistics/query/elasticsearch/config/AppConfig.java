package no.difi.statistics.query.elasticsearch.config;

import no.difi.statistics.QueryService;
import no.difi.statistics.api.QueryRestController;
import no.difi.statistics.query.elasticsearch.ElasticsearchQueryService;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@EnableAutoConfiguration
public class AppConfig {

    @Autowired
    private Environment environment;

    @Bean
    public QueryRestController api() {
        return new QueryRestController(queryService());
    }

    @Bean
    public QueryService queryService() {
        return new ElasticsearchQueryService(elasticSearchClient());
    }

    @Bean(destroyMethod = "close")
    public Client elasticSearchClient() {
        String host = environment.getRequiredProperty("no.difi.statistics.elasticsearch.host");
        int port = environment.getRequiredProperty("no.difi.statistics.elasticsearch.port", Integer.class);
        try {
            return TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to initialize Elasticsearch client", e);
        }
    }

}
