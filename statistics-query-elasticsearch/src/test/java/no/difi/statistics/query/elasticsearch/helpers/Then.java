package no.difi.statistics.query.elasticsearch.helpers;

import static org.junit.Assert.assertEquals;

public class Then<T> {
    private Given given;
    private When when;
    private ThenFunction.Builder<T> thenFunctionBuilder;

    Then(Given given, When when, ThenFunction.Builder<T> thenFunctionBuilder) {
        this.given = given;
        this.when = when;
        this.thenFunctionBuilder = thenFunctionBuilder;
    }


    public Given requestFails(String message) {
        assertEquals(message, when.failure());
        return given;
    }

    public Given isReturned() {
        assertEquals(
                thenFunctionBuilder.build().apply(given.series()),
                when.result()
        );
        return given;
    }

}
