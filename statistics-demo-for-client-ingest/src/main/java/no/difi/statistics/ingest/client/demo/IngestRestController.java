package no.difi.statistics.ingest.client.demo;

import no.difi.statistics.ingest.client.IngestService;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.springframework.boot.SpringApplication;
import org.springframework.web.bind.annotation.*;

import static java.lang.String.format;

@RestController
public class IngestRestController {

    private IngestService service;

    public IngestRestController(IngestService service) {
        this.service = service;
    }

    @RequestMapping(method= RequestMethod.GET, value="/")
    public String index() {
        return format(
                "Statistics Ingest version %s",
                System.getProperty("difi.version", "N/A")
        );
    }

    @RequestMapping(method= RequestMethod.POST, value = "minutes/{seriesName}")
    public void addMinutes(
            @PathVariable String seriesName,
            @RequestBody TimeSeriesPoint dataPoint
    ) {
        service.minute(seriesName, dataPoint);
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(AppConfig.class, args);
    }
}
