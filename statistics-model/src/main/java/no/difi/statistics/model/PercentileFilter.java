package no.difi.statistics.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;

@JsonDeserialize(using = PercentileFilter.JsonDeserializer.class)
public class PercentileFilter {

    private int percentile;
    private String measurementId;
    private RelationalOperator relationalOperator;

    public PercentileFilter(int percentile, String measurementId, RelationalOperator relationalOperator) {
        this.percentile = percentile;
        this.measurementId = measurementId;
        this.relationalOperator = relationalOperator;
    }

    public String getMeasurementId() {
        return measurementId;
    }

    public int getPercentile() {
        return percentile;
    }

    public RelationalOperator getRelationalOperator() {
        return relationalOperator;
    }

    /**
     * Use custom deserializer to maintain immutability property
     */
    static class JsonDeserializer extends com.fasterxml.jackson.databind.JsonDeserializer {

        @Override
        public PercentileFilter deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);
            return new PercentileFilter(
                    node.get("percentile").asInt(),
                    node.get("measurementId").asText(),
                    RelationalOperator.valueOf(node.get("relationalOperator").asText())
            );
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PercentileFilter that = (PercentileFilter) o;

        if (percentile != that.percentile) return false;
        if (!measurementId.equals(that.measurementId)) return false;
        return relationalOperator == that.relationalOperator;
    }

    @Override
    public int hashCode() {
        int result = percentile;
        result = 31 * result + measurementId.hashCode();
        result = 31 * result + relationalOperator.hashCode();
        return result;
    }
}
