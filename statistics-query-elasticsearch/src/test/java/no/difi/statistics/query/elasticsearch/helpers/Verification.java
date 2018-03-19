package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;

public class Verification<R> {

    private Map<String, TimeSeriesGenerator> generators;
    private List<TimeSeries> givenSeries;
    private Query<R> whenQuery;

    private Verification(TimeSeriesGenerator...generators) {
        this.generators = stream(generators).collect(toMap(TimeSeriesGenerator::id, identity()));
    }

    public static WhenStep given(TimeSeriesGenerator...timeSeriesGenerators) {
        return new Flow(new Verification(timeSeriesGenerators));
    }

    private void start() {
        if (givenSeries == null)
            givenSeries = generators.values().stream().map(Supplier::get).collect(toList());
    }

    private List<TimeSeries> givenSeries() {
        return givenSeries;
    }

    public interface WhenStep {
        ThenStep when(Query query);
    }

    public interface ThenStep {
        WhenStep thenThatSeriesIsReturned();
        WhenStep thenThatSeriesIsReturned(boolean log);
        WhenStep thenIsReturned(Query query);
        WhenStep thenIsReturned(Query query, boolean log);
        WhenStep thenFailsWithMessage(String message);
    }

    public static class Flow implements WhenStep, ThenStep {

        private final Logger logger = LoggerFactory.getLogger(getClass());
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
            return thenThatSeriesIsReturned(false);
        }

        @Override
        public WhenStep thenThatSeriesIsReturned(boolean log) {
            Query thenQuery = (Query)verification.whenQuery.toCalculated();
            return thenIsReturned(thenQuery, log);
        }

        @Override
        public WhenStep thenIsReturned(Query thenQuery) {
            return thenIsReturned(thenQuery, false);
        }

        @Override
        public WhenStep thenIsReturned(Query thenQuery, boolean log) {
            verification.start();
            if (log) {
                for (TimeSeries series : (List<TimeSeries>)verification.givenSeries()) {
                    logger.info("Given:\n" + toString(series.getPoints()));
                }
                logger.info("When:\n" + toString(verification.whenQuery.execute()));
                logger.info("Then:\n" + toString(thenQuery.execute(verification.givenSeries())));
            }
            assertEquals(
                    thenQuery.execute(verification.givenSeries()),
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

        private String toString(Object o) {
            if (o instanceof List)
                return toString((List)o);
            else
                return o.toString();
        }

        private String toString(List<?> objects) {
            return objects.stream().map(Object::toString).collect(joining("\n"));
        }

    }

}
