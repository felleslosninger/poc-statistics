package no.difi.statistics.ingest.influxdb.poc;

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
        if (applicationArguments.getOptionNames().size() == 0) return;
        ApplicationArgumentsReader argumentsReader = new ApplicationArgumentsReader(applicationArguments);
        for (ZonedDateTime t = argumentsReader.from(); t.isBefore(argumentsReader.to()); t = t.plusMinutes(1)) {
            URL url = new URL(format(urlTemplate, dtFormatter.format(t), dtFormatter.format(t.plusMinutes(1))));
            try (InputStream response = url.openStream()) {
                JsonReader reader = Json.createReader(response);
                for (JsonValue jsonValue : reader.readArray()) {
                    JsonArray fields = ((JsonObject)jsonValue).getJsonArray("fields");
                    service.minute("idporten-login", dataPoint(t, fields));
                }
            }
        }
    }

    private TimeSeriesPoint dataPoint(ZonedDateTime timestamp, JsonArray fields) {
        String serviceProvider = fields.getJsonObject(0).getString("value");
        return TimeSeriesPoint.builder()
                .timestamp(timestamp)
                .measurement(new Measurement(measurementId(serviceProvider, "MinID"), fields.getJsonObject(4).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "MinID OTC"), fields.getJsonObject(5).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "MinID PIN"), fields.getJsonObject(6).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "BuyPass"), fields.getJsonObject(7).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "Commfides"), fields.getJsonObject(8).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "Federated"), fields.getJsonObject(9).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "BankID"), fields.getJsonObject(10).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "eIDAS"), fields.getJsonObject(11).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "BankID mobil"), fields.getJsonObject(12).getInt("value")))
                .measurement(new Measurement(measurementId(serviceProvider, "Alle"), fields.getJsonObject(13).getInt("value")))
                .build();
    }

    private String measurementId(String serviceProvider, String authenticationMethod) {
        return serviceProvider + " (" + authenticationMethod + ")";
    }
}
