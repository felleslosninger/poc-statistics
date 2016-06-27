package no.difi.statistics.api;

import no.difi.statistics.Statistics;
import no.difi.statistics.TimeSeriesPoint;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.ZonedDateTime;
import java.util.List;

@RestController
public class StatisticsController {

    private Statistics statistics;

    public StatisticsController(Statistics statistics) {
        this.statistics = statistics;
    }

    @GetMapping("/")
    public String index() {
        return "Statistics API";
    }

    @GetMapping("minutes/{seriesName}")
    public List<TimeSeriesPoint> minutes(
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return statistics.minutes(seriesName, from, to);
    }

    @GetMapping("hours/{seriesName}")
    public List<TimeSeriesPoint> hours(
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return statistics.hours(seriesName, from, to);
    }

    @GetMapping("days/{seriesName}")
    public List<TimeSeriesPoint> days(
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return statistics.days(seriesName, from, to);
    }

    @GetMapping("months/{seriesName}")
    public List<TimeSeriesPoint> months(
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return statistics.months(seriesName, from, to);
    }

    @GetMapping("years/{seriesName}")
    public List<TimeSeriesPoint> years(
            @PathVariable String seriesName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) ZonedDateTime to
    ) {
        return statistics.years(seriesName, from, to);
    }

}
