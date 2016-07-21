package no.difi.statistics.ingest;

import no.difi.statistics.model.TimeSeriesPoint;

public interface IngestService {

    void minute(String timeSeriesName, String dataType, TimeSeriesPoint dataPoint);

}
