package no.difi.statistics.ingest.client;

import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

public interface IngestService {
    void ingest(String seriesName, Distance distance, TimeSeriesPoint timeSeriesPoint);
}
