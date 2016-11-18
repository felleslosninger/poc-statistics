package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.ingest.api.IngestResponse;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesDefinition;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.*;
import static java.util.Arrays.stream;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.elasticsearch.QueryBuilders.lastAggregation;
import static no.difi.statistics.elasticsearch.ResultParser.pointFromLastAggregation;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchIngestService implements IngestService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Client client;
    private static final String timeFieldName = "timestamp";
    private static final String indexType = "default";

    public ElasticsearchIngestService(Client client) {
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
            client.prepareIndex(indexName, indexType, id).setSource(document).setCreate(true).get();
        } catch (DocumentAlreadyExistsException e) {
            throw new TimeSeriesPointAlreadyExists(seriesDefinition.getOwner(), seriesDefinition.getName(), id, e);
        }
    }

    @Override
    public IngestResponse ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (TimeSeriesPoint point : dataPoints) {
            bulkRequest.add(
                    client.prepareIndex(
                            resolveIndexName()
                                    .seriesName(seriesDefinition.getName())
                                    .owner(seriesDefinition.getOwner())
                                    .distance(seriesDefinition.getDistance())
                                    .at(normalize(point.getTimestamp(), seriesDefinition.getDistance()))
                                    .single(),
                            indexType,
                            id(point, seriesDefinition.getDistance())
                    )
                            .setSource(document(point, seriesDefinition.getDistance()))
                            .setCreate(true)
            );
        }
        BulkResponse response = bulkRequest.get();
        return response(response);
    }

    @Override
    public TimeSeriesPoint last(TimeSeriesDefinition seriesDefinition) {
        SearchResponse response = searchBuilder(
                resolveIndexName()
                        .seriesName(seriesDefinition.getName())
                        .owner(seriesDefinition.getOwner())
                        .distance(seriesDefinition.getDistance())
                        .list()
        )
                .addAggregation(lastAggregation())
                .setSize(0) // We are after aggregation and not the search hits
                .execute().actionGet();
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

    private static String id(TimeSeriesPoint dataPoint, MeasurementDistance distance) {
        return format(dataPoint.getTimestamp(), distance);
    }

    private static byte[] documentBytes(TimeSeriesPoint dataPoint, MeasurementDistance distance) {
        return document(dataPoint, distance).bytes().toBytes();
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

    private static ZonedDateTime normalize(ZonedDateTime timestamp, MeasurementDistance distance) {
        return timestamp.truncatedTo(chronoUnit(distance)).withZoneSameInstant(UTC);
    }

    private static ChronoUnit chronoUnit(MeasurementDistance distance) {
        switch (distance) {
            case minutes: return MINUTES;
            case hours: return HOURS;
            case days: return DAYS;
            case months: return MONTHS;
            case years: return YEARS;
            default: throw new IllegalArgumentException("Unsupported measurement distance: " + distance);
        }
    }

    private SearchRequestBuilder searchBuilder(List<String> indexNames) {
        return client
                .prepareSearch(indexNames.toArray(new String[indexNames.size()]))
                .addSort(timeFieldName, SortOrder.ASC)
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                .setTypes(indexType);
    }

}
