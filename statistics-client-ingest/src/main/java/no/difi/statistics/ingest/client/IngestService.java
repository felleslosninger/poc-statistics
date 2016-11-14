package no.difi.statistics.ingest.client;

import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

public interface IngestService {
    void post(Series series, String seriesName, TimeSeriesPoint timeSeriesPoint);
}
