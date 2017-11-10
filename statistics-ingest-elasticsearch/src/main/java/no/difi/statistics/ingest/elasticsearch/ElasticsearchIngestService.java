package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.api.IngestResponse;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
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

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final RestHighLevelClient client;
    private static final String timeFieldName = "timestamp";
    private static final String indexType = "default";

    public ElasticsearchIngestService(RestHighLevelClient client) {
        this.client = client;
    }

    @Override
    public void ingest(TimeSeriesDefinition seriesDefinition, TimeSeriesPoint dataPoint) throws TimeSeriesPointAlreadyExists {
        byte[] document = documentBytes(dataPoint, seriesDefinition.getDistance());
        String id = id(dataPoint, seriesDefinition.getDistance());
        String indexName = resolveIndexName()
                .seriesName(seriesDefinition.getName())
                .owner(seriesDefinition.getOwner())
                .distance(seriesDefinition.getDistance())
                .at(normalize(dataPoint.getTimestamp(), seriesDefinition.getDistance())).single();
        log(indexName, id, document);
        try {
            client.index(new IndexRequest(indexName, indexType, id).source(document, JSON).create(true));
        } catch (ElasticsearchStatusException e) {
            if (e.status().equals(RestStatus.CONFLICT))
                throw new TimeSeriesPointAlreadyExists(seriesDefinition.getOwner(), seriesDefinition.getName(), id, e);
            else
                throw new RuntimeException("Failed to index point", e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to index point", e);
        }
    }

    @Override
    public IngestResponse ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints) {
        BulkRequest bulkRequest = new BulkRequest();
        for (TimeSeriesPoint point : dataPoints) {
            bulkRequest.add(
                    new IndexRequest(
                            resolveIndexName()
                                    .seriesName(seriesDefinition.getName())
                                    .owner(seriesDefinition.getOwner())
                                    .distance(seriesDefinition.getDistance())
                                    .at(normalize(point.getTimestamp(), seriesDefinition.getDistance()))
                                    .single(),
                            indexType,
                            id(point, seriesDefinition.getDistance())
                    )
                            .source(documentBytes(point, seriesDefinition.getDistance()))
                            .create(true)
            );
        }
        BulkResponse response = null;
        try {
            response = client.bulk(bulkRequest);
        } catch (IOException e) {
            throw new RuntimeException("Failed to index list of points", e);
        }
        return response(response);
    }

    @Override
    public TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition) {
        List<String> indexNames = resolveIndexName()
                .seriesName(seriesDefinition.getName())
                .owner(seriesDefinition.getOwner())
                .distance(seriesDefinition.getDistance())
                .list();
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
        return failure == null ? IngestResponse.Status.Ok : IngestResponse.Status.Failed;
    }

    private void log(String indexName, String id, byte[] document) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format(
                    "Ingesting: Index=%s Type=%s Id=%s Point=%s",
                    indexName,
                    indexType,
                    id,
                    new String(document, Charset.forName("UTF-8"))
                    )
            );
        }
    }

    private static byte[] documentBytes(TimeSeriesPoint dataPoint, MeasurementDistance distance) {
        return toBytes(document(dataPoint, distance).bytes());
    }

    private static XContentBuilder document(TimeSeriesPoint dataPoint, MeasurementDistance distance) {
        try {
            XContentBuilder builder = jsonBuilder().startObject()
                    .field("timestamp", format(dataPoint.getTimestamp(), distance));
            for (Measurement measurement : dataPoint.getMeasurements()) {
                builder.field(measurement.getId(), measurement.getValue());
            }
            return builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String format(ZonedDateTime timestamp, MeasurementDistance distance) {
        return normalize(timestamp, distance).toString();
    }

}
