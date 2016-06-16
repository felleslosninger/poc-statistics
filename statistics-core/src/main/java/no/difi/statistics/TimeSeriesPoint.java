package no.difi.statistics;

import java.time.ZonedDateTime;

public class TimeSeriesPoint {

    private ZonedDateTime time;
    private int value;

    public TimeSeriesPoint(ZonedDateTime time, int value) {
        this.time = time;
        this.value = value;
    }

    public ZonedDateTime time() {
        return time;
    }

    public int value() {
        return value;
    }

}
