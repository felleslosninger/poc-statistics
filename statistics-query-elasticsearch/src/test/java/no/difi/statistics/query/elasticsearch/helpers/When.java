package no.difi.statistics.query.elasticsearch.helpers;

import java.util.function.Supplier;

public class When<T> {
    private Given given;
    private Supplier<T> supplier;
    private T result;
    private String failure;

    When(Given given, Supplier<T> whenSupplier) {
        this.given = given;
        this.supplier = whenSupplier;
    }

    public String failure() {
        supply();
        return failure;
    }

    public T result() {
        supply();
        return result;
    }

    private void supply() {
        if (result != null || failure != null)
            return;
        try {
            result = supplier.get();
        } catch (RuntimeException e) {
            failure = e.getMessage();
        }
    }

    public Then then(ThenFunction.Builder thenFunctionBuilder) {
        return new Then(given, this, thenFunctionBuilder);
    }
}
