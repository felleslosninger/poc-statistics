package no.difi.statistics.test.utils;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class ElasticsearchHelper {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final String timeFieldName = "timestamp";
    private static final String defaultType = "default";

    private Client client;
    private URL refreshUrl;

    public ElasticsearchHelper(Client client, String host, int port) throws UnknownHostException, MalformedURLException {
        this.client = client;
        this.refreshUrl = new URL(format("http://%s:%d/_refresh", host, port));
    }

    public void clear() {
        client.admin().indices().prepareDelete("_all").get();
    }

    public void refresh() {
        try {
            refreshUrl.openConnection().getContent();
        } catch (IOException e) {
            throw new RuntimeException("Failed to refresh", e);
        }
    }

    public String[] indices() {
        return client.admin().cluster()
                .prepareState().execute()
                .actionGet().getState()
                .getMetaData().concreteAllIndices();
    }

    public void index(String indexName, String indexType, String document) {
        client.prepareIndex(indexName, indexType)
                .setSource(document)
                .setRefresh(true) // Make document immediately searchable for testing purposes
                .get();
    }

    public void index(String indexName, String indexType, String id, Map<String, String> document) {
        client.prepareIndex(indexName, indexType, id)
                .setSource(document)
                .setRefresh(true) // Make document immediately searchable for testing purposes
                .get();
    }

    public SearchResponse search(List<String> indexNames, ZonedDateTime from, ZonedDateTime to) {
        return searchBuilder(indexNames)
                .setQuery(timeRangeQuery(from, to))
                .setSize(10_000) // 10 000 is maximum
                .execute().actionGet();
    }

    public Long get(String indexName, String id, String measurementId) {
        GetResponse response = client.prepareGet(indexName, defaultType, id).get();
        Object value = response.getSource().get(measurementId);
        return value != null && value instanceof Number ? ((Number) value).longValue() : null;
    }

    public void waitConnected() throws InterruptedException {
        for (int i = 0; i < 1000; i++) {
            if (((TransportClient)client).connectedNodes().size() > 0) break;
            Thread.sleep(10L);
        }
    }

    private SearchRequestBuilder searchBuilder(List<String> indexNames) {
        return client
                .prepareSearch(indexNames.toArray(new String[indexNames.size()]))
                .addSort(timeFieldName, SortOrder.ASC)
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(defaultType);
    }

    private RangeQueryBuilder timeRangeQuery(ZonedDateTime from, ZonedDateTime to) {
        RangeQueryBuilder builder = rangeQuery(timeFieldName);
        if (from != null)
            builder.from(dateTimeFormatter.format(from));
        if (to != null)
            builder.to(dateTimeFormatter.format(to));
        return builder;
    }

}
