package no.difi.statistics.query.elasticsearch.config;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.query.QueryService;
import no.difi.statistics.query.config.BackendConfig;
import no.difi.statistics.query.elasticsearch.*;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;

@Configuration
public class ElasticsearchConfig implements BackendConfig {

    private final Environment environment;

    @Autowired
    public ElasticsearchConfig(Environment environment) {
        this.environment = environment;
    }

    @Override
    @Bean
    public QueryService queryService() {
        return new ElasticsearchQueryService(
                elasticsearchClient(),
                commandFactory()
        );
    }

    @Bean
    public CommandFactory commandFactory() {
        return new CommandFactory();
    }

    @Bean
    @Scope("prototype")
    public GetAvailableTimeSeries.Builder listAvailableTimeSeriesCommandBuilder() {
        return GetAvailableTimeSeries.builder().elasticsearchClient(elasticsearchLowLevelClient().build());
    }

    @Bean
    @Scope("prototype")
    public GetLastHistogram.Builder lastHistogramCommandBuilder() {
        return GetLastHistogram.builder().elasticsearchClient(elasticsearchHighLevelClient());
    }

    @Bean
    @Scope("prototype")
    public GetSumHistogram.Builder sumHistogramCommandBuilder() {
        return GetSumHistogram.builder().elasticsearchClient(elasticsearchHighLevelClient());
    }

    @Bean
    @Scope("prototype")
    public GetMeasurementIdentifiers.Builder measurementIdentifiersCommandBuilder() {
        return GetMeasurementIdentifiers.builder().elasticsearchClient(elasticsearchLowLevelClient().build());
    }

    @Bean
    public Client elasticsearchClient() {
        return new Client(
                elasticsearchHighLevelClient(),
                "http://" + elasticsearchHost() + ":" + elasticsearchPort()
        );
    }

    @Bean(destroyMethod = "close")
    public RestHighLevelClient elasticsearchHighLevelClient() {
        return new RestHighLevelClient(elasticsearchLowLevelClient());
    }

    private RestClientBuilder elasticsearchLowLevelClient() {
        String host = environment.getRequiredProperty("no.difi.statistics.elasticsearch.host");
        int port = environment.getRequiredProperty("no.difi.statistics.elasticsearch.port", Integer.class);
        return RestClient.builder(new HttpHost(host, port, "http"));
    }

    private String elasticsearchHost() {
        return environment.getRequiredProperty("no.difi.statistics.elasticsearch.host");
    }

    private int elasticsearchPort() {
        return environment.getRequiredProperty("no.difi.statistics.elasticsearch.port", Integer.class);
    }

}
