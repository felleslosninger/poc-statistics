package no.difi.statistics.ingest;

import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.ingest.IngestResponse;

import java.util.List;

public interface IngestService {

    void minute(String timeSeriesName, String owner, TimeSeriesPoint dataPoint);

    IngestResponse minutes(String timeSeriesName, String owner, List<TimeSeriesPoint> dataPoints);

}
