package no.difi.statistics.api;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;

@JsonDeserialize(using = TimeSeriesFilter.TimeSeriesFilterJsonDeserializer.class)
public class TimeSeriesFilter {

    private int percentile;
    private String measurementId;

    public TimeSeriesFilter(int percentile, String measurementId) {
        this.percentile = percentile;
        this.measurementId = measurementId;
    }

    public String getMeasurementId() {
        return measurementId;
    }

    public int getPercentile() {
        return percentile;
    }

    /**
     * Use custom deserializer to maintain immutability property
     */
    static class TimeSeriesFilterJsonDeserializer extends JsonDeserializer<TimeSeriesFilter> {

        @Override
        public TimeSeriesFilter deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);
            return new TimeSeriesFilter(node.get("percentile").asInt(), node.get("measurementId").asText());
        }

    }

}
