package no.difi.statistics.ingest.elasticsearch;

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
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static no.difi.statistics.elasticsearch.IdResolver.id;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.lastAggregation;
import static no.difi.statistics.elasticsearch.ResultParser.pointFromLastAggregation;
import static no.difi.statistics.elasticsearch.Timestamp.normalize;
import static org.elasticsearch.common.bytes.BytesReference.toBytes;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
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
                            .source(documentBytes(point, seriesDefinition), JSON)
                            .create(true)
            );
        }
        BulkResponse response;
        try {
            response = client.bulk(bulkRequest);
        } catch (IOException e) {
            throw new RuntimeException("Failed to index list of points", e);
        }
        return response(response);
    }

    @Override
    public TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition) {
        List<String> indexNames = resolveIndexName().seriesDefinition(seriesDefinition).list();
        SearchRequest request = new SearchRequest(indexNames.toArray(new String[indexNames.size()]))
                .types(indexType)
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .source(searchSource()
                        .sort(timeFieldName, SortOrder.ASC)
                        .aggregation(lastAggregation())
                        .size(0) // We are after aggregation and not the search hits
                );
        SearchResponse response;
        try {
            response = client.search(request);
        } catch (IOException e) {
            throw new RuntimeException("Failed to search", e);
        }
        return pointFromLastAggregation(response);
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

    private static byte[] documentBytes(TimeSeriesPoint dataPoint, TimeSeriesDefinition seriesDefinition) {
        return toBytes(document(dataPoint, seriesDefinition).bytes());
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
