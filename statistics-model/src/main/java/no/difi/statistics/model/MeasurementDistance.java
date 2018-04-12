package no.difi.statistics.model;

public enum MeasurementDistance {
    minutes,
    hours,
    days,
    months,
    years;

    public boolean lessThan(MeasurementDistance targetDistance) {
        return this.ordinal() < targetDistance.ordinal();
    }

    public boolean greaterThanOrEqualTo(MeasurementDistance targetDistance) {
        return this.ordinal() >= targetDistance.ordinal();
    }

}
