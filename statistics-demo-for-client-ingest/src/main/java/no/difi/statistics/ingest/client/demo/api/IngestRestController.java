package no.difi.statistics.ingest.client.demo.api;

import no.difi.statistics.ingest.client.IngestService;
import no.difi.statistics.ingest.client.demo.config.AppConfig;
import no.difi.statistics.ingest.client.demo.config.BackendConfigURL;
import no.difi.statistics.ingest.client.model.TimeSeriesPoint;
import org.springframework.boot.SpringApplication;
import org.springframework.web.bind.annotation.*;

import static java.lang.String.format;

@RestController
public class IngestRestController {

    private IngestService ingestService;

    public IngestRestController(IngestService service) {
        ingestService = service;
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
        ingestService.minute(seriesName, dataPoint);
    }

    public static void main(String[] args) throws Exception {
        Object[] sources = {AppConfig.class, BackendConfigURL.class};
        SpringApplication.run(sources, args);
    }
}