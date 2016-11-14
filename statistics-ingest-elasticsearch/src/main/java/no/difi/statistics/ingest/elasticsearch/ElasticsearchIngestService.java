package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.ingest.api.IngestResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchIngestService implements IngestService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Client client;
    private static final String indexType = "default";
    private static ChronoUnit chronoUnit = MINUTES;

    public ElasticsearchIngestService(Client client) {
        this.client = client;
    }

    @Override
    public void minute(String series, String owner, TimeSeriesPoint dataPoint) throws TimeSeriesPointAlreadyExists {
        chronoUnit = MINUTES;
        byte[] document = documentBytes(dataPoint);
        String id = id(dataPoint);
        String indexName = resolveIndexName().seriesName(series).owner(owner).minutes().at(normalize(dataPoint.getTimestamp())).single();
        log(indexName, id, document);
        try {
            client.prepareIndex(indexName, indexType, id).setSource(document).setCreate(true).get();
        } catch (DocumentAlreadyExistsException e) {
            throw new TimeSeriesPointAlreadyExists(owner, series, id, e);
        }
    }

    @Override
    public IngestResponse minutes(String timeSeriesName, String owner, List<TimeSeriesPoint> dataPoints) {
        chronoUnit = MINUTES;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (TimeSeriesPoint point : dataPoints) {
            bulkRequest.add(
                    client.prepareIndex(
                            resolveIndexName()
                                    .seriesName(timeSeriesName)
                                    .owner(owner)
                                    .minutes()
                                    .at(normalize(point.getTimestamp()))
                                    .single(),
                            indexType,
                            id(point)
                    )
                            .setSource(document(point))
                            .setCreate(true)
            );
        }
        BulkResponse response = bulkRequest.get();
        return response(response);
    }

    @Override
    public IngestResponse hour(String timeSeriesName, String owner, TimeSeriesPoint datapoint) {
        chronoUnit = HOURS;
        byte[] document = documentBytes(datapoint);
        String id = id(datapoint);
        String indexName = resolveIndexName().seriesName(timeSeriesName).owner(owner).hours().at(normalize(datapoint.getTimestamp())).single();
        log(indexName, id, document);
        try {
            client.prepareIndex(indexName, indexType, id).setSource(document).setCreate(true).get();
        } catch (DocumentAlreadyExistsException e) {
            throw new TimeSeriesPointAlreadyExists(owner, timeSeriesName, id, e);
        }
        return null;
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

    private static String id(TimeSeriesPoint dataPoint) {
        return format(dataPoint.getTimestamp());
    }

    private static byte[] documentBytes(TimeSeriesPoint dataPoint) {
        return document(dataPoint).bytes().toBytes();
    }

    private static XContentBuilder document(TimeSeriesPoint dataPoint) {
        try {
            XContentBuilder builder = jsonBuilder().startObject()
                    .field("timestamp", format(dataPoint.getTimestamp()));
            for (Measurement measurement : dataPoint.getMeasurements()) {
                builder.field(measurement.getId(), measurement.getValue());
            }
            return builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String format(ZonedDateTime timestamp) {
        return normalize(timestamp).toString();
    }

    private static ZonedDateTime normalize(ZonedDateTime timestamp) {
        return timestamp.truncatedTo(chronoUnit).withZoneSameInstant(UTC);
    }
}
