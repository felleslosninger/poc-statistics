package no.difi.statistics.test.utils;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.elasticsearch.IdResolver;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import org.apache.http.ConnectionClosedException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.test.utils.DataOperations.unit;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class ElasticsearchHelper {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final String timeFieldName = "timestamp";
    private static final String defaultType = "default";
    private final static String aMeasurementId = "count";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Client client;

    public ElasticsearchHelper(Client client) throws UnknownHostException, MalformedURLException {
        this.client = client;
    }

    public static GenericContainer startContainer() {
        GenericContainer container = null;
        try {
            container = new GenericContainer("elasticsearch:5.6.3").withCommand("-Enetwork.host=_site_");
        } catch (Exception e) {
            // try again as
            container = new GenericContainer("elasticsearch:5.6.3").withCommand("-Enetwork.host=_site_");
        }
        container.start();
        return container;
    }

    public void clear() {
        try {
            client.lowLevel().performRequest("DELETE", "/_all");
        } catch (ConnectException|ConnectionClosedException e) {
            // Assume Elasticsearch is stopped
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear Elasticsearch state", e);
        }
    }

    public void refresh() {
        try {
            client.lowLevel().performRequest("GET", "/_refresh");
        } catch (IOException e) {
            throw new RuntimeException("Failed to refresh", e);
        }
    }

    public String[] indices() {
        List<String> indices = new ArrayList<>();
        try (InputStream response = client.lowLevel().performRequest("GET", "/_cat/indices?h=index").getEntity().getContent();
             Scanner scanner = new Scanner(response)) {
            scanner.forEachRemaining(indices::add);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get indices", e);
        }
        return indices.toArray(new String[indices.size()]);
    }

    public void index(String indexName, String indexType, String id, String document) {
        try {
            client.highLevel().index(new IndexRequest(indexName, indexType, id)
                    .source(document, JSON)
                    .create(true)
                    .setRefreshPolicy(IMMEDIATE)); // Make document immediately searchable for testing purposes
        } catch (IOException e) {
            throw new RuntimeException("Failed to index", e);
        }
    }

    public void index(String indexName, String indexType, String id, Map<String, String> document) {
        try {
            client.highLevel().index(new IndexRequest(indexName, indexType, id)
                    .source(document, JSON)
                    .setRefreshPolicy(IMMEDIATE)); // Make document immediately searchable for testing purposes
        } catch (IOException e) {
            throw new RuntimeException("Failed to index", e);
        }
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
        SearchRequest request = new SearchRequest(indexNames.toArray(new String[indexNames.size()]))
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .types(defaultType)
                .source(new SearchSourceBuilder()
                        .sort(timeFieldName, SortOrder.ASC)
                        .query(timeRangeQuery(from, to))
                        .size(10_000)); // 10 000 is maximum
        try {
            return client.highLevel().search(request);
        } catch (IOException e) {
            throw new RuntimeException("Search failed", e);
        }
    }

    public Long get(String indexName, String id, String measurementId) {
        GetResponse response;
        try {
            response = client.highLevel().get(new GetRequest(indexName, defaultType, id));
        } catch (IOException e) {
            throw new RuntimeException("Failed to get document", e);
        }
        Object value = response.getSource().get(measurementId);
        return value != null && value instanceof Number ? ((Number) value).longValue() : null;
    }

    public void waitForGreenStatus() {
        try {
            for (int i = 0; i < 100; i++) {
                try {
                    client.lowLevel().performRequest("GET", "/_cluster/health?wait_for_status=green&timeout=50s");
                    return;
                } catch (IOException e) {
                    logger.info("Waiting for green status (" + i + "/1000): " + e.toString());
                    Thread.sleep(100);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to check Elasticsearch status", e);
        }

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
