package no.difi.statistics.query.api;

import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

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

    @RequestMapping("minutes/{seriesName}")
    public List<TimeSeriesPoint> minutes(
            @PathVariable String seriesName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.minutes(seriesName, from, to);
    }

    @RequestMapping(method = RequestMethod.POST, path = "minutes/{seriesName}")
    public List<TimeSeriesPoint> minutesAbovePercentile(
            @PathVariable String seriesName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestBody TimeSeriesFilter filter
    ) {
        return service.minutes(seriesName, from, to, filter);
    }

    @RequestMapping("hours/{seriesName}")
    public List<TimeSeriesPoint> hours(
            @PathVariable String seriesName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.hours(seriesName, from, to);
    }

    @RequestMapping("days/{seriesName}")
    public List<TimeSeriesPoint> days(
            @PathVariable String seriesName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.days(seriesName, from, to);
    }

    @RequestMapping("months/{seriesName}")
    public List<TimeSeriesPoint> months(
            @PathVariable String seriesName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.months(seriesName, from, to);
    }

    @RequestMapping("years/{seriesName}")
    public List<TimeSeriesPoint> years(
            @PathVariable String seriesName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.years(seriesName, from, to);
    }

    @RequestMapping("point/{seriesName}")
    public TimeSeriesPoint point(
            @PathVariable String seriesName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.point(seriesName, from, to);
    }

}
