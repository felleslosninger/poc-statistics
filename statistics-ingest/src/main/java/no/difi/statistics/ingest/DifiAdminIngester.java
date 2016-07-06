package no.difi.statistics.ingest;

import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.elasticsearch.client.Client;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static java.lang.String.format;

public class DifiAdminIngester extends AbstractIngester {

    private final static String urlTemplate =
            "https://admin.difi.eon.no/idporten-admin/statistics/statistics/json/r1/%s/to/%s";
    private final static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm");

    public DifiAdminIngester(Client client) {
        super(client);
    }

    @Override
    protected void ingest(ZonedDateTime from, ZonedDateTime to) throws IOException {
        for (ZonedDateTime t = from; t.isBefore(to); t = t.plusMinutes(1)) {
            URL url = new URL(format(urlTemplate, dtFormatter.format(t), dtFormatter.format(t.plusMinutes(1))));
            try (InputStream response = url.openStream()) {
                JsonReader reader = Json.createReader(response);
                for (JsonValue jsonValue : reader.readArray()) {
                    JsonArray fields = ((JsonObject)jsonValue).getJsonArray("fields");
                    indexTimeSeriesPoint(
                            indexNameForMinuteSeries("idporten-login", t),
                            mappingTypeName(fields.getJsonObject(0).getString("value")),
                            dataPoint(t, fields)
                    );
                }
            }
        }
    }

    private String mappingTypeName(String input) {
        return input.replaceAll(",", ""); // Commas not allowed in Elasticsearch mapping type name
    }

    private TimeSeriesPoint dataPoint(ZonedDateTime timestamp, JsonArray fields) {
        return TimeSeriesPoint.builder()
                .timestamp(timestamp)
                .measurement(new Measurement("MinID", fields.getJsonObject(4).getInt("value")))
                .measurement(new Measurement("MinID OTC", fields.getJsonObject(5).getInt("value")))
                .measurement(new Measurement("MinID PIN", fields.getJsonObject(6).getInt("value")))
                .measurement(new Measurement("BuyPass", fields.getJsonObject(7).getInt("value")))
                .measurement(new Measurement("Commfides", fields.getJsonObject(8).getInt("value")))
                .measurement(new Measurement("Federated", fields.getJsonObject(9).getInt("value")))
                .measurement(new Measurement("BankID", fields.getJsonObject(10).getInt("value")))
                .measurement(new Measurement("eIDAS", fields.getJsonObject(11).getInt("value")))
                .measurement(new Measurement("BankID mobil", fields.getJsonObject(12).getInt("value")))
                .measurement(new Measurement("Total", fields.getJsonObject(13).getInt("value")))
                .build();
    }

}
