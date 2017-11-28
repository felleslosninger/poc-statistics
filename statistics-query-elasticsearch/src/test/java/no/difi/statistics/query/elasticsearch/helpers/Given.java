package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.TimeSeries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Given {

    private Map<String, Supplier<TimeSeries>> suppliers = new HashMap<>();

    private Given(TimeSeriesGenerator... timeSeriesGenerators) {
        this.suppliers = stream(timeSeriesGenerators).collect(toMap(TimeSeriesGenerator::id, identity()));
    }

    public static Given given(TimeSeriesGenerator... timeSeriesGenerators) {
        return new Given(timeSeriesGenerators);
    }

    List<TimeSeries> series() {
        return suppliers.values().stream().map(Supplier::get).collect(toList());
    }

    public <T> When<T> when(Supplier<T> whenSupplier) {
        return new When<>(this, whenSupplier);
    }

}
