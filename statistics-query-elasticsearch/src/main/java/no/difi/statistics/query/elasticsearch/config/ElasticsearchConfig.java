package no.difi.statistics.query.elasticsearch.config;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.query.QueryService;
import no.difi.statistics.query.config.BackendConfig;
import no.difi.statistics.query.elasticsearch.*;
import no.difi.statistics.query.elasticsearch.commands.*;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
        return new ElasticsearchQueryService(commandFactory());
    }

    @Bean
    public CommandFactory commandFactory() {
        return new CommandFactory();
    }

    @Bean
    @Scope("prototype")
    public TimeSeriesQuery.Builder queryCommandBuilder() {
        return TimeSeriesQuery.builder().elasticsearchClient(elasticsearchHighLevelClient()).sumHistogramCommand(sumHistogramCommandBuilder());
    }

    @Bean
    @Scope("prototype")
    public AvailableSeriesQuery.Builder listAvailableTimeSeriesCommandBuilder() {
        return AvailableSeriesQuery.builder().elasticsearchClient(elasticsearchLowLevelClient().build());
    }

    @Bean
    @Scope("prototype")
    public LastHistogramQuery.Builder lastHistogramCommandBuilder() {
        return LastHistogramQuery.builder().elasticsearchClient(elasticsearchHighLevelClient());
    }

    @Bean
    @Scope("prototype")
    public LastQuery.Builder lastCommandBuilder() {
        return LastQuery.builder().elasticsearchClient(elasticsearchHighLevelClient());
    }

    @Bean
    @Scope("prototype")
    public SumHistogramQuery.Builder sumHistogramCommandBuilder() {
        return SumHistogramQuery.builder().elasticsearchClient(elasticsearchHighLevelClient());
    }

    @Bean
    @Scope("prototype")
    public SumQuery.Builder sumCommandBuilder() {
        return SumQuery.builder().elasticsearchClient(elasticsearchHighLevelClient());
    }

    @Bean
    @Scope("prototype")
    public PercentileQuery.Builder percentileCommandBuilder() {
        return PercentileQuery.builder().elasticsearchClient(elasticsearchHighLevelClient());
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
                getScheme()+"://" + elasticsearchHost() + ":" + elasticsearchPort()
        );
    }

    @Bean(destroyMethod = "close")
    public RestHighLevelClient elasticsearchHighLevelClient() {
        return new RestHighLevelClient(elasticsearchLowLevelClient());
    }

    private RestClientBuilder elasticsearchLowLevelClient() {
        String host = environment.getRequiredProperty("no.difi.statistics.elasticsearch.host");
        int port = environment.getRequiredProperty("no.difi.statistics.elasticsearch.port", Integer.class);
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, getScheme()));
        Header[] headers = new Header[]{new BasicHeader("Authorization","ApiKey " + loadApiKey())};
        builder.setDefaultHeaders(headers);
        return  builder;
    }

    private String getScheme() {
        String scheme = "http";
        // TODO put this into config?
        if (elasticsearchHost() != null && elasticsearchHost().endsWith("elastic-cloud.com")) {
            scheme = "https";
        }
        return scheme;
    }

    private String loadApiKey(){
        final String fileName = environment.getRequiredProperty("file.base.difi-statistikk");
        try {
            final Path pathToPasswordFile = Paths.get(fileName);
            return new String(Files.readAllBytes(pathToPasswordFile));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load file defined in environment property 'file.base.difi-statistikk': " + fileName, e);
        }
    }

    private String elasticsearchHost() {
        return environment.getRequiredProperty("no.difi.statistics.elasticsearch.host");
    }

    private int elasticsearchPort() {
        return environment.getRequiredProperty("no.difi.statistics.elasticsearch.port", Integer.class);
    }

}
