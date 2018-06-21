package no.difi.statistics.elasticsearch;

import no.difi.statistics.model.TimeRange;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.ZonedDateTime;
import java.util.List;

import static java.time.ZoneOffset.UTC;
import static java.time.ZonedDateTime.now;
import static no.difi.statistics.elasticsearch.IndexNameResolver.resolveIndexName;
import static no.difi.statistics.model.TimeSeriesDefinition.builder;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class IndexNameResolverTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void givenYearSeriesWhenResolvingThenResultIsOneNameWithoutDate() {
        List<String> indexNames = resolveIndexName().seriesDefinition(builder().name("test").years().owner("owner"))
                .range(new TimeRange(now(), now())).list();
        assertThat(indexNames, contains("owner@test@year"));
    }

    @Test
    public void givenMinuteSeriesWithinDayWhenResolvingThenResultIsOneNameWithYear() {
        List<String> indexNames = resolveIndexName().seriesDefinition(builder().name("test").minutes().owner("owner"))
                .range(new TimeRange(timestamp(2016, 3, 22, 1, 23), timestamp(2016, 3, 22, 17, 18))).list();
        assertThat(indexNames, contains("owner@test@minute2016"));
    }

    @Test
    public void givenMonthSeriesWithinYearWhenResolvingThenResultIsOneNameWithYear() {
        List<String> indexNames = resolveIndexName().seriesDefinition(builder().name("test").months().owner("owner"))
                .range(new TimeRange(timestamp(2016, 1, 22), timestamp(2016, 6, 30))).list();
        assertThat(indexNames, contains("owner@test@month2016"));
    }

    @Test
    public void givenMonthSeriesCrossingYearsWhenResolvingThenResultIsOneNamePerYear() {
        List<String> indexNames = resolveIndexName().seriesDefinition(builder().name("test").months().owner("owner"))
                .range(new TimeRange(timestamp(2014, 1, 22), timestamp(2016, 6, 30))).list();
        assertThat(indexNames, contains("owner@test@month2014", "owner@test@month2015", "owner@test@month2016"));
    }

    private ZonedDateTime timestamp(int year, int month, int day) {
        return ZonedDateTime.of(year, month, day, 0, 0, 0, 0, UTC);
    }

    private ZonedDateTime timestamp(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, UTC);
    }

}
