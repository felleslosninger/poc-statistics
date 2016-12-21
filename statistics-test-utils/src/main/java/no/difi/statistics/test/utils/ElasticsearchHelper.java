package no.difi.statistics.test.utils;

import no.difi.statistics.elasticsearch.IdResolver;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.test.utils.DataOperations.unit;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.health.ClusterHealthStatus.GREEN;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class ElasticsearchHelper {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final String timeFieldName = "timestamp";
    private static final String defaultType = "default";
    private final static String aMeasurementId = "count";

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
                .getMetaData().getConcreteAllIndices();
    }

    public void index(String indexName, String indexType, String id, String document) {
        client.prepareIndex(indexName, indexType, id)
                .setSource(document)
                .setCreate(true)
                .setRefreshPolicy(IMMEDIATE) // Make document immediately searchable for testing purposes
                .get();
    }

    public void index(String indexName, String indexType, String id, Map<String, String> document) {
        client.prepareIndex(indexName, indexType, id)
                .setSource(document)
                .setRefreshPolicy(IMMEDIATE) // Make document immediately searchable for testing purposes
                .get();
    }

    public void indexPoints(MeasurementDistance distance, List<TimeSeriesPoint> points) throws IOException {
        indexPoints("test_owner", "test", distance, points);
    }

    public void indexPoints(String owner, String series, MeasurementDistance distance, List<TimeSeriesPoint> points) throws IOException {
        for (TimeSeriesPoint point : points)
            indexPoint(indexNameForSeries(owner, series, distance, point.getTimestamp()), IdResolver.id(point, distance), point);
    }

    public List<TimeSeriesPoint> indexPointsFrom(ZonedDateTime timestamp, MeasurementDistance distance, long...values) throws IOException {
        return indexPointsFrom(timestamp, "test_owner", "test", distance, values);
    }

    public List<TimeSeriesPoint> indexPointsFrom(ZonedDateTime timestamp, String owner, String series, MeasurementDistance distance, long... values) throws IOException {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            TimeSeriesPoint point = TimeSeriesPoint.builder().timestamp(timestamp).measurement(aMeasurementId, value).build();
            points.add(point);
            indexPoint(indexNameForSeries(owner, series, distance, timestamp), IdResolver.id(point, distance), point);
            timestamp = timestamp.plus(1, unit(distance));
        }
        return points;
    }

    public TimeSeriesPoint indexPoint(String series, MeasurementDistance distance, ZonedDateTime timestamp, long value) throws IOException {
        return indexPoint("test_owner", series, distance, timestamp, value);
    }

    public TimeSeriesPoint indexPoint(MeasurementDistance distance, ZonedDateTime timestamp, long value) throws IOException {
        return indexPoint("test_owner", "test", distance, timestamp, value);
    }

    public TimeSeriesPoint indexPoint(String owner, String series, MeasurementDistance distance, ZonedDateTime timestamp, long value) throws IOException {
        TimeSeriesPoint point = TimeSeriesPoint.builder().timestamp(timestamp).measurement(aMeasurementId, value).build();
        indexPoint(indexNameForSeries(owner, series, distance, timestamp), IdResolver.id(point, distance), point);
        return point;
    }

    private void indexPoint(String indexName, String id, TimeSeriesPoint point) throws IOException {
        XContentBuilder sourceBuilder = jsonBuilder().startObject()
                .field("timestamp", formatTimestamp(point.getTimestamp()));
        for (Measurement measurement : point.getMeasurements())
            sourceBuilder.field(measurement.getId(), measurement.getValue());
        index(indexName, "default", id, sourceBuilder.endObject().string());
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

    public void waitForGreenStatus() throws InterruptedException, ExecutionException {
        for (int i = 0; i < 1000; i++) {
            if (((TransportClient)client).connectedNodes().size() > 0) break;
            Thread.sleep(10L);
        }
        for (int i = 0; i < 1000; i++) {
            if (client.admin().cluster().health(new ClusterHealthRequest()).get().getStatus().equals(GREEN))
                break;
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

    private String indexNameForSeries(String owner, String series, MeasurementDistance distance, ZonedDateTime timestamp) {
        return resolveIndexName().seriesName(series).owner(owner).distance(distance).at(timestamp).single();
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

}
