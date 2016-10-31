package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.ingest.IngestResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static java.lang.String.format;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchIngestService implements IngestService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Client client;
    private final static String indexType = "default";

    public ElasticsearchIngestService(Client client) {
        this.client = client;
    }

    @Override
    public void minute(String timeSeriesName, String owner, TimeSeriesPoint dataPoint) {
        indexTimeSeriesPoint(
                resolveIndexName().seriesName(timeSeriesName).owner(owner).minutes().at(dataPoint.getTimestamp()).single(),
                indexType,
                dataPoint
        );
    }

    @Override
    public IngestResponse minutes(String timeSeriesName, String owner, List<TimeSeriesPoint> dataPoints) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (TimeSeriesPoint point : dataPoints) {
            bulkRequest.add(
                    client.prepareIndex(
                            resolveIndexName()
                                    .seriesName(timeSeriesName)
                                    .owner(owner)
                                    .minutes()
                                    .at(point.getTimestamp())
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

    private IngestResponse response(BulkResponse response) {
        IngestResponse.Builder ingestResponse = IngestResponse.builder();
        response.iterator().forEachRemaining(i -> ingestResponse.status(status(i.getFailure())));
        return ingestResponse.build();
    }

    private IngestResponse.Status status(BulkItemResponse.Failure failure) {
        return failure == null ? IngestResponse.Status.Ok : IngestResponse.Status.Failed;
    }

    private static String id(TimeSeriesPoint point) {
        return formatTimestamp(point.getTimestamp());
    }

    private void indexTimeSeriesPoint(String indexName, String indexType, TimeSeriesPoint dataPoint) {
        byte[] document = documentBytes(dataPoint);
        logger.info(format("Ingesting: Index=%s Type=%s Point=%s", indexName, indexType, new String(document, Charset.forName("UTF-8"))));
        if (indexType == null || indexType.trim().isEmpty()) {
            logger.warn("Ignoring point without type");
            return;
        }
        client.prepareIndex(indexName, indexType).setSource(document).get();
    }

    private static byte[] documentBytes(TimeSeriesPoint dataPoint) {
        return document(dataPoint).bytes().toBytes();
    }

    private static XContentBuilder document(TimeSeriesPoint dataPoint) {
        try {
            XContentBuilder builder = jsonBuilder().startObject()
                    .field("timestamp", formatTimestamp(dataPoint.getTimestamp()));
            for (Measurement measurement : dataPoint.getMeasurements()) {
                builder.field(measurement.getId(), measurement.getValue());
            }
            return builder.endObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

}
