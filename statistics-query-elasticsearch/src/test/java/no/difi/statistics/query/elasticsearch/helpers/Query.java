package no.difi.statistics.query.elasticsearch.helpers;

import no.difi.statistics.model.TimeSeries;

import java.util.List;
import java.util.function.Function;

public abstract class Query<R> {

    private transient Function<List<TimeSeries>, R> function;
    private transient R result;
    private transient String failure;

    public abstract <T> T toCalculated();

    public Query<R> function(Function<List<TimeSeries>, R> function) {
        this.function = function;
        return this;
    }

    public synchronized String failure() {
        if (isNotCached()) {
            try {
                doExecute(null);
            } catch (RuntimeException e) {
                // Ignore, we want the failure as return
            }
        }
        return failure;
    }

    public synchronized R execute(List<TimeSeries> timeSeries) {
        if (isNotCached())
            doExecute(timeSeries);
        return result;
    }

    public synchronized R execute() {
        if (isNotCached())
            doExecute(null);
        return result;
    }

    private boolean isNotCached() {
        return result == null && failure == null;
    }

    private void doExecute(List<TimeSeries> input) {
        try {
            result = function.apply(input);
        } catch (RuntimeException e) {
            failure = e.getMessage();
            throw e;
        }
    }

}
