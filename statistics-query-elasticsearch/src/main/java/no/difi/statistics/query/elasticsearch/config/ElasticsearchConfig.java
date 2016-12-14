package no.difi.statistics.query.elasticsearch.config;

import no.difi.statistics.query.QueryService;
import no.difi.statistics.query.config.BackendConfig;
import no.difi.statistics.query.elasticsearch.ElasticsearchQueryService;
import no.difi.statistics.query.elasticsearch.ListAvailableTimeSeries;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class ElasticsearchConfig implements BackendConfig {

    @Autowired
    private Environment environment;

    @Override
    @Bean
    public QueryService queryService() {
        return new ElasticsearchQueryService(
                elasticSearchClient(),
                listAvailableTimeSeriesCommand()
        );
    }

    @Bean
    public ListAvailableTimeSeries.Command listAvailableTimeSeriesCommand() {
        return ListAvailableTimeSeries.builder().elasticsearchClient(elasticSearchClient());
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
