package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.elasticsearch.Timestamp;
import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.api.IngestResponse;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static no.difi.statistics.elasticsearch.IdResolver.id;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.Timestamp.normalize;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.BucketOrder.key;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

public class ElasticsearchIngestService implements IngestService {

    private final RestHighLevelClient client;
    private static final String timeFieldName = "timestamp";
    private static final String indexType = "default";

    public ElasticsearchIngestService(RestHighLevelClient client) {
        this.client = client;
    }

    @Override
    public IngestResponse ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints) {
        BulkRequest bulkRequest = new BulkRequest();
        for (TimeSeriesPoint point : dataPoints) {
            bulkRequest.add(
                    new IndexRequest(
                            resolveIndexName()
                                    .seriesDefinition(seriesDefinition)
                                    .at(normalize(point.getTimestamp(), seriesDefinition.getDistance()))
                                    .single(),
                            indexType,
                            id(point, seriesDefinition)
                    )
                            .source(document(point, seriesDefinition))
                            .create(false) // false->tillate update av requests. true->feilar på same request fleire gonger
            );
        }
        BulkResponse response;
        try {
            response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Failed to index list of points", e);
        }
        return response(response);
    }

    @Override
    public TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition) {
        List<String> indexNames = resolveIndexName().seriesDefinition(seriesDefinition).list();
        SearchRequest request = new SearchRequest(indexNames.toArray(new String[0]))
                .types(indexType)
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .source(searchSource()
                        .sort(timeFieldName, SortOrder.ASC)
                        .aggregation(terms("last").field("timestamp").size(10_000).order(key(false)).size(1))
                        .size(0) // We are after aggregation and not the search hits
                );
        SearchResponse response;
        try {
            response = client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Failed to search", e);
        }
        return pointFromLastAggregation(response);
    }

    private static TimeSeriesPoint pointFromLastAggregation(SearchResponse response) {
        if (response.getAggregations() == null)
            return null;
        if (response.getAggregations().get("last") == null)
            throw new RuntimeException("No last aggregation in result");
        if (response.getAggregations().<Terms>get("last").getBuckets().size() == 0)
            return null;
        if (response.getAggregations().<Terms>get("last").getBuckets().size() > 1)
            throw new RuntimeException("Too many buckets in last aggregation: "
                    + response.getAggregations().<Terms>get("last").getBuckets().size());
        Terms.Bucket bucket = response.getAggregations().<Terms>get("last").getBuckets().get(0);
        return TimeSeriesPoint.builder()
                .timestamp(Timestamp.parse(bucket.getKeyAsString()))
                .build();
    }

    private IngestResponse response(BulkResponse response) {
        IngestResponse.Builder ingestResponse = IngestResponse.builder();
        response.iterator().forEachRemaining(i -> ingestResponse.status(status(i.getFailure())));
        return ingestResponse.build();
    }

    private IngestResponse.Status status(BulkItemResponse.Failure failure) {
        if (failure == null)
            return IngestResponse.Status.Ok;
        switch (failure.getStatus()) {
            case OK: return IngestResponse.Status.Ok;
            case CONFLICT: return IngestResponse.Status.Conflict;
            default: return IngestResponse.Status.Failed;
        }
    }

    private static XContentBuilder document(TimeSeriesPoint dataPoint, TimeSeriesDefinition seriesDefinition) {
        try {
            XContentBuilder builder = jsonBuilder().startObject();
            addField(builder, timeFieldName, format(dataPoint.getTimestamp(), seriesDefinition.getDistance()));
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

    private static String format(ZonedDateTime timestamp, MeasurementDistance distance) {
        return normalize(timestamp, distance).toString();
    }

}
