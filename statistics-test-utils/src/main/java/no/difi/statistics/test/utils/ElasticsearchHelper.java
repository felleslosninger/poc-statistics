package no.difi.statistics.test.utils;

import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.elasticsearch.IdResolver;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
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
        GenericContainer container;
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
        indexPoints(TimeSeriesDefinition.builder().name("test").distance(distance).owner("test_owner"), points);
    }

    public void indexPoints(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> points) throws IOException {
        for (TimeSeriesPoint point : points)
            indexPoint(seriesDefinition, point);
    }

    public List<TimeSeriesPoint> indexPointsFrom(ZonedDateTime timestamp, MeasurementDistance distance, long...values) throws IOException {
        return indexPointsFrom(timestamp, TimeSeriesDefinition.builder().name("test").distance(distance).owner("test_owner"), values);
    }

    private List<TimeSeriesPoint> indexPointsFrom(ZonedDateTime timestamp, TimeSeriesDefinition seriesDefinition, long... values) throws IOException {
        List<TimeSeriesPoint> points = new ArrayList<>(values.length);
        for (long value : values) {
            TimeSeriesPoint point = TimeSeriesPoint.builder().timestamp(timestamp).measurement(aMeasurementId, value).build();
            points.add(point);
            indexPoint(seriesDefinition, point);
            timestamp = timestamp.plus(1, unit(seriesDefinition.getDistance()));
        }
        return points;
    }

    public TimeSeriesPoint indexPoint(MeasurementDistance distance, ZonedDateTime timestamp, long value) throws IOException {
        return indexPoint(
                TimeSeriesDefinition.builder().name("test").distance(distance).owner("test_owner"),
                TimeSeriesPoint.builder().timestamp(timestamp).measurement(aMeasurementId, value).build()
        );
    }

    private TimeSeriesPoint indexPoint(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint point) throws IOException {
        XContentBuilder document = document(point, seriesDefinition);
        index(
                indexNameForSeries(seriesDefinition, point.getTimestamp()),
                "default",
                IdResolver.id(point, seriesDefinition), document.string()
        );
        return point;
    }

    private static XContentBuilder document(TimeSeriesPoint dataPoint, TimeSeriesDefinition seriesDefinition) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            addField(builder, timeFieldName, formatTimestamp(dataPoint.getTimestamp()));
            dataPoint.getCategories().ifPresent(cs -> cs.forEach((key, value) -> addCategoryField(builder, key, value)));
            dataPoint.getMeasurements().forEach(m -> addMeasurementField(builder, m.getId(), m.getValue()));
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
        logger.info("Waiting for Elasticsearch to have green status...");
        long t0 = System.currentTimeMillis();
        try {
            for (int i = 0; i < 200; i++) {
                try {
                    client.lowLevel().performRequest("GET", "/_cluster/health?wait_for_status=green&timeout=50s");
                    logger.info("Green status after " + ((System.currentTimeMillis() - t0) / 1000) + " seconds (i=" + i + ")");
                    return;
                } catch (IOException e) {
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
