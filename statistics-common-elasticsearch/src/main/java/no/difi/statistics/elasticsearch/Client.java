package no.difi.statistics.elasticsearch;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Client {

    private RestHighLevelClient highLevelClient;
    private RestClient lowLevelClient;

    public Client(RestHighLevelClient highLevelClient, RestClient lowLevelClient) {
        this.highLevelClient = highLevelClient;
        this.lowLevelClient = lowLevelClient;
    }

    public RestHighLevelClient highLevel() {
        return highLevelClient;
    }

    public RestClient lowLevel() {
        return lowLevelClient;
    }

}
