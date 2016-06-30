package no.difi.statistics.ingest;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Random;

public class RandomIngester extends AbstractIngester {

    public RandomIngester(Client client) {
        super(client);
    }

    @Override
    protected void ingest(ZonedDateTime from, ZonedDateTime to) throws IOException {
        Random random = new Random();
        for (ZonedDateTime t = from; t.isBefore(to); t = t.plusMinutes(1)) {
            int value = random.nextInt();
            indexTimeSeriesPoint(
                    indexNameForMinuteSeries("random", t),
                    "total",
                    TimeSeriesPoint.builder().timestamp(t).measurement(new Measurement("count", value)).build()
            );
        }
    }

}
