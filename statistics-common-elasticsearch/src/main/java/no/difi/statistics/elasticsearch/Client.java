package no.difi.statistics.elasticsearch;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Client {

    private RestHighLevelClient highLevelClient;
    private RestClient lowLevelClient;
    private String address;

    public Client(RestHighLevelClient highLevelClient, RestClient lowLevelClient, String address) {
        this.highLevelClient = highLevelClient;
        this.lowLevelClient = lowLevelClient;
        this.address = address;
    }

    public RestHighLevelClient highLevel() {
        return highLevelClient;
    }

    public RestClient lowLevel() {
        return lowLevelClient;
    }

    public String toString() {
        return address;
    }

}
