package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.TimeSeries;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

public class Verification<R> {

    private Map<String, TimeSeriesGenerator> generators;
    private List<TimeSeries> generatedSeries;
    private Query<R> whenQuery;

    private Verification(TimeSeriesGenerator...generators) {
        this.generators = stream(generators).collect(toMap(TimeSeriesGenerator::id, identity()));
    }

    public static WhenStep given(TimeSeriesGenerator...timeSeriesGenerators) {
        return new Flow(new Verification(timeSeriesGenerators));
    }

    private void start() {
        if (generatedSeries == null)
            generatedSeries = generators.values().stream().map(Supplier::get).collect(toList());
    }

    private List<TimeSeries> generatedSeries() {
        return generatedSeries;
    }

    public interface WhenStep {
        ThenStep when(Query query);
    }

    public interface ThenStep {
        WhenStep thenThatSeriesIsReturned();
        WhenStep thenIsReturned(Query query);
        WhenStep thenFailsWithMessage(String message);
    }

    public static class Flow implements WhenStep, ThenStep {

        private Verification verification;

        Flow(Verification verification) {
            this.verification = verification;
        }

        @Override
        public ThenStep when(Query query) {
            verification.whenQuery = query;
            return this;
        }

        @Override
        public WhenStep thenThatSeriesIsReturned() {
            Query thenQuery = (Query)verification.whenQuery.toCalculated();
            return thenIsReturned(thenQuery);
        }

        @Override
        public WhenStep thenIsReturned(Query thenQuery) {
            verification.start();
            assertEquals(
                    thenQuery.execute(verification.generatedSeries()),
                    verification.whenQuery.execute()
            );
            return this;
        }

        @Override
        public WhenStep thenFailsWithMessage(String message) {
            verification.start();
            assertEquals(message, verification.whenQuery.failure());
            return this;
        }

    }

}
