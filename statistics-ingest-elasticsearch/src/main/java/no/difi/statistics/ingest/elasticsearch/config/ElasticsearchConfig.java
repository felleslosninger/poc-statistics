package no.difi.statistics.ingest.elasticsearch.config;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.config.BackendConfig;
import no.difi.statistics.ingest.elasticsearch.ElasticsearchIngestService;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

    @Bean
    public IngestService ingestService() {
        return new ElasticsearchIngestService(elasticsearchHighLevelClient());
    }

    @Bean
    public Client elasticsearchClient() {
        return new Client(
                elasticsearchHighLevelClient(),
                getHttpScheme()+"://" + elasticsearchHost() + ":" + elasticsearchPort()
        );
    }

    @Bean(destroyMethod = "close")
    public RestHighLevelClient elasticsearchHighLevelClient() {
        return new RestHighLevelClient(elasticsearchLowLevelClient());
    }

    private RestClientBuilder elasticsearchLowLevelClient() {
        String host = environment.getRequiredProperty("no.difi.statistics.elasticsearch.host");
        int port = environment.getRequiredProperty("no.difi.statistics.elasticsearch.port", Integer.class);

        Header[] headers = new Header[]{new BasicHeader("Authorization","ApiKey " + loadApiKey())};
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, getHttpScheme()));
        builder.setDefaultHeaders(headers);
        return builder;
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


    private String getHttpScheme(){
        String scheme = "http";
        // TODO put this into config?
        if (elasticsearchHost().endsWith("elastic-cloud.com")) {
            scheme = "https";
        }
        return scheme;
    }

}
