package no.difi.statistics.test.utils;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.elasticsearch.IdResolver;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.apache.http.ConnectionClosedException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static no.difi.statistics.elasticsearch.IdResolver.id;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.Timestamp.normalize;
import static no.difi.statistics.test.utils.DataOperations.unit;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
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

    public ElasticsearchHelper(Client client) {
        this.client = client;
    }

    public void clear() {
        Request request = new Request("DELETE", "/_all");
        try {
            client.lowLevel().performRequest(request);
        } catch (ConnectException|ConnectionClosedException e) {
            // Assume Elasticsearch is stopped
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear Elasticsearch state", e);
        }
    }

    public void refresh() {
        Request request = new Request("GET", "/_refresh");
        try {
            client.lowLevel().performRequest(request);
        } catch (IOException e) {
            throw new RuntimeException("Failed to refresh", e);
        }
    }

    public String[] indices() {
        List<String> indices = new ArrayList<>();
        Request request = new Request("GET", "/_cat/indices?h=index");
        try (InputStream response = client.lowLevel().performRequest(request).getEntity().getContent();
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
                    .setRefreshPolicy(IMMEDIATE), RequestOptions.DEFAULT); // Make document immediately searchable for testing purposes
        } catch (IOException e) {
            throw new RuntimeException("Failed to index", e);
        }
    }

    public void index(String indexName, String indexType, String id, Map<String, String> document) {
        try {
            client.highLevel().index(new IndexRequest(indexName, indexType, id)
                    .source(document, JSON)
                    .setRefreshPolicy(IMMEDIATE), RequestOptions.DEFAULT); // Make document immediately searchable for testing purposes
        } catch (IOException e) {
            throw new RuntimeException("Failed to index", e);
        }
    }

    public void indexPoints(MeasurementDistance distance, List<TimeSeriesPoint> points) {
        indexPoints(TimeSeriesDefinition.builder().name("test").distance(distance).owner("test_owner"), points);
    }

    public void indexPoints(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> points) {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(IMMEDIATE).timeout(timeValueMinutes(1));
        points.forEach(point -> bulkRequest.add(indexRequest(seriesDefinition, point)));
        try {
            BulkResponse response = client.highLevel().bulk(bulkRequest, RequestOptions.DEFAULT);
            if (response.hasFailures())
                throw new RuntimeException("Failed to bulk index points: " + response.buildFailureMessage());
        } catch (IOException e) {
            throw new RuntimeException("Failed to bulk index points", e);
        }
    }

    public List<TimeSeriesPoint> indexPointsFrom(ZonedDateTime timestamp, MeasurementDistance distance, long...values) {
        return indexPointsFrom(timestamp, TimeSeriesDefinition.builder().name("test").distance(distance).owner("test_owner"), values);
    }

    private List<TimeSeriesPoint> indexPointsFrom(ZonedDateTime timestamp, TimeSeriesDefinition seriesDefinition, long... values) {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            points.add(TimeSeriesPoint.builder().timestamp(timestamp).measurement(aMeasurementId, value).build());
            timestamp = timestamp.plus(1, unit(seriesDefinition.getDistance()));
        }
        indexPoints(seriesDefinition, points);
        return points;
    }

    public TimeSeriesPoint indexPoint(MeasurementDistance distance, ZonedDateTime timestamp, long value) throws IOException {
        return indexPoint(
                TimeSeriesDefinition.builder().name("test").distance(distance).owner("test_owner"),
                TimeSeriesPoint.builder().timestamp(timestamp).measurement(aMeasurementId, value).build()
        );
    }

    private TimeSeriesPoint indexPoint(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint point) throws IOException {
        XContentBuilder document = document(point);
        index(
                indexNameForSeries(seriesDefinition, point.getTimestamp()),
                "default",
                IdResolver.id(point, seriesDefinition), Strings.toString(document)
        );
        return point;
    }

    private IndexRequest indexRequest(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint point) {
        return new IndexRequest(
                resolveIndexName()
                        .seriesDefinition(seriesDefinition)
                        .at(normalize(point.getTimestamp(), seriesDefinition.getDistance()))
                        .single()
                ,"default",
                id(point, seriesDefinition)
        )
                .source(Strings.toString(document(point)), JSON)
                .create(true);
    }

    private static XContentBuilder document(TimeSeriesPoint dataPoint) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            addField(builder, timeFieldName, formatTimestamp(dataPoint.getTimestamp()));
            dataPoint.getCategories().ifPresent(cs -> cs.forEach((key, value) -> addCategoryField(builder, key, value)));
            dataPoint.getMeasurements().forEach((key, value) -> addMeasurementField(builder, key, value));
            return builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addMeasurementField(XContentBuilder builder, String id, long value) {
        if (id.startsWith("category.")) throw new IllegalArgumentException("Measurement ids cannot be prefixed with \"category.\"");
        if (id.equals(timeFieldName)) throw new IllegalArgumentException("Measurement ids cannot be named \"" + timeFieldName + "\"");
        addField(builder, id, value);
    }

    private static void addCategoryField(XContentBuilder builder, String key, String value) {
        addField(builder, "category." + key, value);
    }

    private static void addField(XContentBuilder builder, String key, Object value) {
        try {
            builder.field(key, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            return client.highLevel().search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Search failed", e);
        }
    }

    public Long get(String indexName, String id, String measurementId) {
        GetResponse response;
        try {
            response = client.highLevel().get(new GetRequest(indexName, defaultType, id), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get document", e);
        }
        Object value = response.getSource().get(measurementId);
        return value != null && value instanceof Number ? ((Number) value).longValue() : null;
    }

    public void waitForGreenStatus() {
        logger.info("Waiting for Elasticsearch to have green status (" + client + ")...");
        long t0 = System.currentTimeMillis();
        Request request = new Request("GET", "/_cluster/health?wait_for_status=green&timeout=10s");
        try {
            for (int i = 0; i < 200; i++) {
                try {
                    client.lowLevel().performRequest(request );
                    logger.info("Green status after " + ((System.currentTimeMillis() - t0) / 1000) + " seconds (i=" + i + ")");
                    return;
                } catch (IOException e) {
                    if (i >= 199)
                        logger.error("Could not get green status with client " + client + ". Reason is: " + e);
                    else
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

    private String indexNameForSeries(TimeSeriesDefinition seriesDefinition, ZonedDateTime timestamp) {
        return resolveIndexName().seriesDefinition(seriesDefinition).at(timestamp).single();
    }

    private static String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

}
