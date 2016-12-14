package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeries;
import no.difi.statistics.model.TimeSeriesPoint;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Given {

    private Map<String, Supplier<TimeSeries>> suppliers = new HashMap<>();

    private Given(GivenSupplier... givenSuppliers) {
        this.suppliers = stream(givenSuppliers).collect(toMap(GivenSupplier::id, identity()));
    }

    public static Given given(GivenSupplier...givenSuppliers) {
        return new Given(givenSuppliers);
    }

    TimeSeries seriesForDistance(MeasurementDistance distance) {
        return suppliers.get(distance.toString()).get();
    }

    List<TimeSeries> series() {
        return suppliers.values().stream().map(Supplier::get).collect(toList());
    }

    public <T> When<T> when(Supplier<T> whenSupplier) {
        return new When<>(this, whenSupplier);
    }

}
