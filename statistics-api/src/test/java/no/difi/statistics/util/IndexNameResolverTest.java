package no.difi.statistics.util;

import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.List;

import static java.time.ZoneOffset.UTC;
import static no.difi.statistics.util.IndexNameResolver.resolveMinuteIndexNames;
import static no.difi.statistics.util.IndexNameResolver.resolveMonthIndexNames;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class IndexNameResolverTest {

    @Test
    public void givenMinuteSeriesWithinDayWhenResolvingThenResultIsOneNameWithDay() {
        List<String> indexNames = resolveMinuteIndexNames("test", timestamp(2016, 03, 22, 01, 23), timestamp(2016, 03, 22, 17, 18));
        assertThat(indexNames, contains("test:minute2016.03.22"));
    }

    @Test
    public void givenMonthSeriesWithinYearWhenResolvingThenResultIsOneNameWithYear() {
        List<String> indexNames = resolveMonthIndexNames("test", timestamp(2016, 01, 22), timestamp(2016, 06, 30));
        assertThat(indexNames, contains("test:month2016"));
    }

    @Test
    public void givenMonthSeriesCrossingYearsWhenResolvingThenResultIsOneNamePerYear() {
        List<String> indexNames = resolveMonthIndexNames("test", timestamp(2014, 01, 22), timestamp(2016, 06, 30));
        assertThat(indexNames, contains("test:month2014", "test:month2015", "test:month2016"));
    }

    private ZonedDateTime timestamp(int year, int month, int day) {
        return ZonedDateTime.of(year, month, day, 0, 0, 0, 0, UTC);
    }

    private ZonedDateTime timestamp(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, UTC);
    }

}
