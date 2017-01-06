package no.difi.statistics.ingest.client;

import no.difi.statistics.ingest.client.model.TimeSeriesDefinition;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;

import java.util.List;

public interface IngestService {
    void ingest(String seriesName, Distance distance, TimeSeriesPoint timeSeriesPoint);

    void ingest(TimeSeriesDefinition seriesDefinition, List<TimeSeriesPoint> dataPoints);
}
