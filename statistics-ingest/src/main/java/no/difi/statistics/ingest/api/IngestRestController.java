package no.difi.statistics.ingest.api;

import no.difi.statistics.ingest.IngestService;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.ingest.IngestResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

@RestController
public class IngestRestController {

    private IngestService ingestService;

    public IngestRestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @GetMapping(value="/")
    public String index() {
        return format(
                "Statistics Ingest version %s",
                System.getProperty("difi.version", "N/A")
        );
    }

    @ExceptionHandler(IngestService.TimeSeriesPointAlreadyExists.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public void alreadyExists() {
        // Do nothing
    }


    @PostMapping(
            value = "{owner}/{seriesName}/minute",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public void minute(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestBody TimeSeriesPoint dataPoint
    ) {
        ingestService.minute(seriesName, owner, dataPoint);
    }

    @PostMapping(
            value = "{owner}/{seriesName}/minutes",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public IngestResponse minutes(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestBody List<TimeSeriesPoint> dataPoints
    ) {
        return ingestService.minutes(seriesName, owner, dataPoints);
    }

}
