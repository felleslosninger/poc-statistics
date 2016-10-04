package no.difi.statistics.ingest.elasticsearch;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static java.lang.String.format;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticsearchIngestService implements IngestService {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Client client;

    public ElasticsearchIngestService(Client client) {
        this.client = client;
    }

    @Override
    public void minute(String timeSeriesName, String owner, TimeSeriesPoint dataPoint) {
        indexTimeSeriesPoint(
                resolveIndexName().seriesName(timeSeriesName).owner(owner).minutes().at(dataPoint.getTimestamp()).single(),
                "default",
                dataPoint
        );
    }

    private void indexTimeSeriesPoint(String indexName, String indexType, TimeSeriesPoint dataPoint) {
        byte[] document = document(dataPoint);
        logger.info(format("Ingesting: Index=%s Type=%s Point=%s", indexName, indexType, new String(document, Charset.forName("UTF-8"))));
        if (indexType == null || indexType.trim().isEmpty()) {
            logger.warn("Ignoring point without type");
            return;
        }
        client.prepareIndex(indexName, indexType).setSource(document).get();
    }

    private static byte[] document(TimeSeriesPoint dataPoint) {
        try {
            XContentBuilder builder = jsonBuilder().startObject()
                    .field("timestamp", formatTimestamp(dataPoint.getTimestamp()));
            for (Measurement measurement : dataPoint.getMeasurements()) {
                builder.field(measurement.getId(), measurement.getValue());
            }
            return builder.endObject().bytes().toBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

}
