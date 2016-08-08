package no.difi.statistics.ingest.client.model;

public class Measurement {

    private final String id;
    private final int value;

    public Measurement(String id, int value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Measurement{" +
                "id='" + id + '\'' +
                ", value=" + value +
                '}';
    }

}
