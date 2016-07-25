package no.difi.statistics.query.api;

import no.difi.statistics.query.QueryService;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static java.lang.String.format;

@RestController
public class QueryRestController {

    private QueryService service;

    public QueryRestController(QueryService service) {
        this.service = service;
    }

    @RequestMapping("/")
    public String index() throws IOException {
        return format(
                "Statistics API version %s",
                System.getProperty("difi.version", "N/A")
        );
    }

    @RequestMapping("minutes/{seriesName}/{type}")
    public List<TimeSeriesPoint> minutes(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.minutes(seriesName, type, from, to);
    }

    @RequestMapping(method = RequestMethod.POST, path = "minutes/{seriesName}/{type}")
    public List<TimeSeriesPoint> minutesAbovePercentile(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestBody TimeSeriesFilter filter
    ) {
        return service.minutes(seriesName, type, from, to, filter);
    }

    @RequestMapping("hours/{seriesName}/{type}")
    public List<TimeSeriesPoint> hours(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.hours(seriesName, type, from, to);
    }

    @RequestMapping("days/{seriesName}/{type}")
    public List<TimeSeriesPoint> days(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.days(seriesName, type, from, to);
    }

    @RequestMapping("months/{seriesName}/{type}")
    public List<TimeSeriesPoint> months(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.months(seriesName, type, from, to);
    }

    @RequestMapping("years/{seriesName}/{type}")
    public List<TimeSeriesPoint> years(
            @PathVariable String seriesName,
            @PathVariable String type,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.years(seriesName, type, from, to);
    }

}
