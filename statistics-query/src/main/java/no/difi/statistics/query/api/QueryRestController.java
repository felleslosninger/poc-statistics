package no.difi.statistics.query.api;

import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.model.query.TimeSeriesFilter;
import no.difi.statistics.query.QueryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

@RestController
public class QueryRestController {

    private QueryService service;

    public QueryRestController(QueryService service) {
        this.service = service;
    }

    @GetMapping("/")
    public RedirectView index() throws IOException {
        return new RedirectView("swagger-ui.html");
    }

    @GetMapping("minutes/{owner}")
    public List<String> timeSeries(
        @PathVariable String owner)
    {
        return service.availableTimeSeries(owner);
    }

    @GetMapping("minutes/{owner}/{seriesName}/last")
    public TimeSeriesPoint last(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    )
    {
        return service.last(seriesName, owner, from, to);
    }

    @GetMapping("minutes/{owner}/{seriesName}")
    public List<TimeSeriesPoint> minutes(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.minutes(seriesName, owner, from, to);
    }

    @PostMapping("minutes/{owner}/{seriesName}")
    public List<TimeSeriesPoint> minutesAbovePercentile(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to,
            @RequestBody TimeSeriesFilter filter
    ) {
        return service.minutes(seriesName, owner, from, to, filter);
    }

    @GetMapping("hours/{owner}/{seriesName}")
    public List<TimeSeriesPoint> hours(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.hours(seriesName, owner, from, to);
    }

    @GetMapping("days/{owner}/{seriesName}")
    public List<TimeSeriesPoint> days(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.days(seriesName, owner, from, to);
    }

    @GetMapping("months/{owner}/{seriesName}")
    public List<TimeSeriesPoint> months(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.months(seriesName, owner, from, to);
    }

    @GetMapping("months/{owner}/{seriesName}/last")
    public List<TimeSeriesPoint> lastInMonths(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.lastInMonths(seriesName, owner, from, to);
    }

    @GetMapping("years/{owner}/{seriesName}")
    public List<TimeSeriesPoint> years(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.years(seriesName, owner, from, to);
    }

    @GetMapping("point/{owner}/{seriesName}")
    public TimeSeriesPoint point(
            @PathVariable String owner,
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return service.point(seriesName, owner, from, to);
    }

}
