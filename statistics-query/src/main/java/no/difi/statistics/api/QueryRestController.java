package no.difi.statistics.api;

import no.difi.statistics.QueryService;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpRequest;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static java.lang.String.format;

@RestController
public class QueryRestController {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private QueryService service;

    public QueryRestController(QueryService service) {
        this.service = service;
    }

    @ExceptionHandler
    public void handleException(HttpRequest request, Exception exception) {
        logger.error("Request " + request.getMethod() + " " + request.getURI() + " failed", exception);
    }

    @GetMapping("/")
    public String index() throws IOException {
        return format(
                "Statistics API version %s",
                System.getProperty("difi.version", "N/A")
        );
    }

    @GetMapping("minutes/{seriesName}/{type}")
    public List<TimeSeriesPoint> minutes(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.minutes(seriesName, type, from, to);
    }

    @PostMapping("minutes/{seriesName}/{type}")
    public List<TimeSeriesPoint> minutesAbovePercentile(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestBody TimeSeriesFilter filter
    ) {
        return service.minutes(seriesName, type, from, to, filter);
    }

    @GetMapping("hours/{seriesName}/{type}")
    public List<TimeSeriesPoint> hours(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.hours(seriesName, type, from, to);
    }

    @GetMapping("days/{seriesName}/{type}")
    public List<TimeSeriesPoint> days(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.days(seriesName, type, from, to);
    }

    @GetMapping("months/{seriesName}/{type}")
    public List<TimeSeriesPoint> months(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.months(seriesName, type, from, to);
    }

    @GetMapping("years/{seriesName}/{type}")
    public List<TimeSeriesPoint> years(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.years(seriesName, type, from, to);
    }

}
