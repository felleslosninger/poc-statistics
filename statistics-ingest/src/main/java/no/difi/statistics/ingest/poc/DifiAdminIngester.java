package no.difi.statistics.ingest.poc;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.Measurement;
import no.difi.statistics.model.TimeSeriesPoint;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import javax.json.*;
import java.io.InputStream;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static java.lang.String.format;

public class DifiAdminIngester implements ApplicationRunner {

    private final static String urlTemplate =
            "https://admin.difi.eon.no/idporten-admin/statistics/statistics/json/r1/%s/to/%s";
    private final static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm");
    private IngestService service;

    public DifiAdminIngester(IngestService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        if (!applicationArguments.containsOption("from")) return;
        ApplicationArgumentsReader argumentsReader = new ApplicationArgumentsReader(applicationArguments);
        for (ZonedDateTime t = argumentsReader.from(); t.isBefore(argumentsReader.to()); t = t.plusMinutes(1)) {
            URL url = new URL(format(urlTemplate, dtFormatter.format(t), dtFormatter.format(t.plusMinutes(1))));
            try (InputStream response = url.openStream()) {
                JsonReader reader = Json.createReader(response);
                for (JsonValue jsonValue : reader.readArray()) {
                    JsonArray fields = ((JsonObject)jsonValue).getJsonArray("fields");
                    service.minute(timeSeriesName(), dataPoint(t, fields));
                }
            }
        }
    }

    private String timeSeriesName() {
        return "idporten-login";
    }

    private TimeSeriesPoint dataPoint(ZonedDateTime timestamp, JsonArray fields) {
        return TimeSeriesPoint.builder()
                .timestamp(timestamp)
                .measurement(measurement("MinID", 4, fields))
                .measurement(measurement("MinID OTC", 5, fields))
                .measurement(measurement("MinID PIN", 6, fields))
                .measurement(measurement("BuyPass", 7, fields))
                .measurement(measurement("Commfides", 8, fields))
                .measurement(measurement("Federated", 9, fields))
                .measurement(measurement("BankID", 10, fields))
                .measurement(measurement("eIDAS", 11, fields))
                .measurement(measurement("BankID mobil", 12, fields))
                .measurement(measurement("Alle", 13, fields))
                .build();
    }

    private Measurement measurement(String authenticationMethod, int index, JsonArray fields) {
        return new Measurement(authenticationMethod + "[" + value(0, fields) + "]", value(index, fields));
    }

    private int value(int index, JsonArray fields) {
        return fields.getJsonObject(index).getInt("value");
    }

}
