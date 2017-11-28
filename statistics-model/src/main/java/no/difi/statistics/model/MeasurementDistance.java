package no.difi.statistics.model;

public enum MeasurementDistance {
    minutes,
    hours,
    days,
    months,
    years;

    public boolean lessThanOrEqualTo(MeasurementDistance targetDistance) {
        return this.ordinal() <= targetDistance.ordinal();
    }

    public boolean greaterThan(MeasurementDistance targetDistance) {
        return this.ordinal() > targetDistance.ordinal();
    }

}
