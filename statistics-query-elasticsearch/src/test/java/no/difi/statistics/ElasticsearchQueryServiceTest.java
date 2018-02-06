package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.model.*;
import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.elasticsearch.config.ElasticsearchConfig;
import no.difi.statistics.query.elasticsearch.helpers.Given;
import no.difi.statistics.query.elasticsearch.helpers.TimeSeriesGenerator;
import no.difi.statistics.query.elasticsearch.helpers.WhenSupplier;
import no.difi.statistics.test.utils.DataOperations;
import no.difi.statistics.test.utils.ElasticsearchHelper;
import no.difi.statistics.test.utils.ElasticsearchRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static no.difi.statistics.ElasticsearchQueryServiceTest.PercentileFilterBuilder.*;
import static no.difi.statistics.elasticsearch.Timestamp.truncatedTimestamp;
import static no.difi.statistics.model.MeasurementDistance.*;
import static no.difi.statistics.model.RelationalOperator.*;
import static no.difi.statistics.query.elasticsearch.helpers.Given.given;
import static no.difi.statistics.query.elasticsearch.helpers.ThenFunction.*;
import static no.difi.statistics.query.elasticsearch.helpers.ThenFunction.sum;
import static no.difi.statistics.query.elasticsearch.helpers.ThenFunction.sumPer;
import static no.difi.statistics.test.utils.DataGenerator.createRandomTimeSeries;
import static no.difi.statistics.test.utils.DataOperations.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = {AppConfig.class, ElasticsearchConfig.class}, initializers = ElasticsearchQueryServiceTest.Initializer.class)
@RunWith(SpringRunner.class)
public class ElasticsearchQueryServiceTest {

    @ClassRule
    public static ElasticsearchRule elasticsearchRule = new ElasticsearchRule();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            EnvironmentTestUtils.addEnvironment(
                    applicationContext.getEnvironment(),
                    "no.difi.statistics.elasticsearch.host=" + elasticsearchRule.getHost(),
                    "no.difi.statistics.elasticsearch.port=" + elasticsearchRule.getPort()
            );
        }

    }

    private final static ZoneId UTC = ZoneId.of("UTC");
    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 0, 0, 0, 0, UTC);
    private final static String measurementId = "count";
    private final static String series = "test";
    private final static String owner = "test_owner"; // Index names must be lower case in Elasticsearch

    @Autowired
    private Client client;
    @Autowired
    private TestRestTemplate restTemplate;
    private ElasticsearchHelper helper;
    @Autowired
    private ObjectMapper objectMapper;

    @Before
    public void prepare() {
        helper = new ElasticsearchHelper(client);
        helper.waitForGreenStatus();
    }

    @After
    public void cleanup() {
        helper.clear();
    }

    @Test
    public void givenTimeSeriesWhenQueryingForAvailableTimeSeriesThenAvailableTimeSeriesAreReturned() {
        given(
                aSeries().withName("series1").withDistance(minutes).withOwner("owner1"),
                aSeries().withName("series1").withDistance(hours).withOwner("owner2"),
                aSeries().withName("series1").withDistance(days).withOwner("owner3"),
                aSeries().withName("series1").withDistance(months).withOwner("owner4"),
                aSeries().withName("series1").withDistance(years).withOwner("owner5")
        )
                .when(this::requestingAvailableTimeSeries)
                .then(availableSeries()).isReturned();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(minutes);
    }

    @Test
    public void givenHourSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(hours);
    }

    @Test
    public void givenDaySeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(days);
    }

    @Test
    public void givenMonthSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(months);
    }

    @Test
    public void givenYearSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(years);
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(minutes);
    }

    @Test
    public void givenHourSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(hours);
    }

    @Test
    public void givenDaySeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(days);
    }

    @Test
    public void givenMonthSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(months);
    }

    @Test
    public void givenYearSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned() throws Exception {
        givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(years);
    }

    private void givenSeriesWhenQueryingForLastPointWithinRangeThenThatPointIsReturned(MeasurementDistance distance) throws IOException {
        helper.indexPointsFrom(dateTime(2003, 1, 1, 11, 11), distance, 1, 2, 3, 4, 5, 6, 7, 8, 9); // Some random "old" points
        TimeSeriesPoint expectedLastPoint = helper.indexPoint(distance, dateTime(2016, 3, 3, 12, 12), 123L);
        helper.indexPoint(distance, dateTime(2017, 4, 4, 1, 2), 5675L); // Point after last
        TimeSeriesPoint actualLastPoint = requestLast(series, distance, owner, dateTime(2007, 1, 1, 0, 0), dateTime(2016, 3, 3, 13, 0));
        assertEquals(expectedLastPoint, actualLastPoint);
    }

    private void givenSeriesWhenQueryingForLastPointWithoutRangeThenThatPointIsReturned(MeasurementDistance distance) throws IOException {
        helper.indexPointsFrom(dateTime(2003, 1, 1, 11, 11), distance, 1, 2, 3, 4, 5, 6, 7, 8, 9); // Some random "old" points
        TimeSeriesPoint expectedLastPoint = helper.indexPoint(distance, dateTime(2016, 3, 3, 12, 12), 123L);
        TimeSeriesPoint actualLastPoint = requestLast(series, distance, owner);
        assertEquals(expectedLastPoint, actualLastPoint);
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusMinutes(1003), minutes, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(minutes, series, now.minusMinutes(1002), now.minusMinutes(1001));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesLastingTwoDaysWhenQueryingForRangeOverThoseDaysThenAllDataPointsAreReturned() throws IOException {
        helper.indexPoint(minutes, now.minusDays(1), 13);
        helper.indexPoint(minutes, now, 117);
        List<TimeSeriesPoint> timeSeries = request(minutes, series, now.minusDays(1).minusHours(1), now);
        assertEquals(13, measurementValue(measurementId, 0, timeSeries));
        assertEquals(117, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusMinutes(20), minutes, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(minutes, series, now.minusMinutes(9), now.minusMinutes(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() {
        given(aSeries().withDistance(minutes).from(now.minusMinutes(400)))
                .when(requestingPoints().withDistance(minutes).from(now.minusYears(10)).to(now.plusYears(10)))
                .then(series()).isReturned();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithoutRangeThenAllDataPointsAreReturned() {
        given(aSeries().withDistance(minutes))
                .when(requestingPoints().withDistance(minutes))
                .then(series()).isReturned();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithCategoryThenPointsWithThatCategoryAreReturned() {
        given(aSeries().withDistance(minutes).withCategories("Category A=Value for category A", "Category B=Value for category B"))
                .when(requestingPoints().withDistance(minutes).withCategory("Category A=Value for category A"))
                .then(series().withDistance(minutes).withCategory("Category A", "Value for category A")).isReturned();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithoutCategoryThenPointsWithSummarizedCategoriesAreReturned() {
        given(aSeries().withDistance(minutes).withCategories("a=b", "c=d"))
                .when(requestingPoints().withDistance(minutes))
                .then(series().withDistance(minutes)).isReturned();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithLeftOpenRangeThenCorrectDataPointsAreReturned() throws IOException {
        helper.indexPoint(minutes, now.minusMinutes(100), 100);
        helper.indexPoint(minutes, now.minusMinutes(200), 200);
        helper.indexPoint(minutes, now.minusMinutes(300), 300);
        helper.indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = requestTo(minutes, series, now.minusMinutes(101));
        assertEquals(3, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithRightOpenRangeThenCorrectDataPointsAreReturned() throws IOException {
        helper.indexPoint(minutes, now.minusMinutes(100), 100);
        helper.indexPoint(minutes, now.minusMinutes(200), 200);
        helper.indexPoint(minutes, now.minusMinutes(300), 300);
        helper.indexPoint(minutes, now.minusMinutes(400), 400);
        List<TimeSeriesPoint> timeSeries = requestFrom(minutes, series, now.minusMinutes(101));
        assertEquals(1, size(timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusHours(1003), hours, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(hours, series, now.minusHours(1002), now.minusHours(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusHours(20), hours, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(hours, series, now.minusHours(9), now.minusHours(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenHourSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException {
        helper.indexPoint(hours, now.minusHours(100), 100);
        helper.indexPoint(hours, now.minusHours(200), 200);
        helper.indexPoint(hours, now.minusHours(300), 300);
        helper.indexPoint(hours, now.minusHours(400), 400);
        List<TimeSeriesPoint> timeSeries = request(hours, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusDays(1003), days, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(days, series, now.minusDays(1002), now.minusDays(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusDays(20), days, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(days, series, now.minusDays(9), now.minusDays(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenDaySeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException {
        helper.indexPoint(days, now.minusDays(100), 100);
        helper.indexPoint(days, now.minusDays(200), 200);
        helper.indexPoint(days, now.minusDays(300), 300);
        helper.indexPoint(days, now.minusDays(400), 400);
        List<TimeSeriesPoint> timeSeries = request(days, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusMonths(1003), months, 1003, 1002, 1001, 1000);
        List<TimeSeriesPoint> timeSeries = request(months, series, now.minusMonths(1002), now.minusMonths(1001));
        assertEquals(2, size(timeSeries));
        assertEquals(1002, measurementValue(measurementId, 0, timeSeries));
        assertEquals(1001, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusMonths(20), months, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(months, series, now.minusMonths(9), now.minusMonths(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenMonthSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException {
        helper.indexPoint(months, now.minusMonths(10), 100);
        helper.indexPoint(months, now.minusMonths(20), 200);
        helper.indexPoint(months, now.minusMonths(30), 300);
        helper.indexPoint(months, now.minusMonths(40), 400);
        List<TimeSeriesPoint> timeSeries = request(months, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeInsideSeriesThenCorrespondingDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusYears(4), years, 4, 3, 2, 1);
        List<TimeSeriesPoint> timeSeries = request(years, series, now.minusYears(3), now.minusYears(2));
        assertEquals(2, size(timeSeries));
        assertEquals(3, measurementValue(measurementId, 0, timeSeries));
        assertEquals(2, measurementValue(measurementId, 1, timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForRangeOutsideSeriesThenNoDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusYears(20), years, 20, 19, 18, 17);
        List<TimeSeriesPoint> timeSeries = request(years, series, now.minusYears(9), now.minusYears(8));
        assertEquals(0, size(timeSeries));
    }

    @Test
    public void givenYearSeriesWhenQueryingForApproxUnlimitedRangeThenAllDataPointsAreReturned() throws IOException {
        helper.indexPointsFrom(now.minusYears(4), years, 4, 3, 2, 1);
        List<TimeSeriesPoint> timeSeries = request(years, series, now.minusYears(10), now.plusYears(10));
        assertEquals(4, size(timeSeries));
    }

    @Test
    public void givenMinuteSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        given(aSeries().withDistance(minutes).withMeasurements("m1", "m2", "m3", "m4"))
            .when(requestingPoints().withDistance(minutes).withMeasurement("m1").lessThanPercentile(92))
                .then(percentile().as(lessThan(92).forMeasurement("m1"))).isReturned()
            .when(requestingPoints().withDistance(minutes).withMeasurement("m2").greaterThanPercentile(43))
                .then(percentile().as(greaterThan(43).forMeasurement("m2"))).isReturned()
            .when(requestingPoints().withDistance(minutes).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then(percentile().as(lessThanOrEqualTo(15).forMeasurement("m3"))).isReturned()
            .when(requestingPoints().withDistance(minutes).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then(percentile().as(greaterThanOrEqualTo(57).forMeasurement("m4"))).isReturned();
    }

    @Test
    public void givenHourSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        given(aSeries().withDistance(hours).withMeasurements("m1", "m2", "m3", "m4"))
            .when(requestingPoints().withDistance(hours).withMeasurement("m1").lessThanPercentile(92))
                .then(percentile().as(lessThan(92).forMeasurement("m1"))).isReturned()
            .when(requestingPoints().withDistance(hours).withMeasurement("m2").greaterThanPercentile(43))
                .then(percentile().as(greaterThan(43).forMeasurement("m2"))).isReturned()
            .when(requestingPoints().withDistance(hours).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then(percentile().as(lessThanOrEqualTo(15).forMeasurement("m3"))).isReturned()
            .when(requestingPoints().withDistance(hours).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then(percentile().as(greaterThanOrEqualTo(57).forMeasurement("m4"))).isReturned();
    }

    @Test
    public void givenDaySeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        given(aSeries().withDistance(days).withMeasurements("m1", "m2", "m3", "m4"))
            .when(requestingPoints().withDistance(days).withMeasurement("m1").lessThanPercentile(92))
                .then(percentile().as(lessThan(92).forMeasurement("m1")).fromSeriesWithDistance(days)).isReturned()
            .when(requestingPoints().withDistance(days).withMeasurement("m2").greaterThanPercentile(43))
                .then(percentile().as(greaterThan(43).forMeasurement("m2")).fromSeriesWithDistance(days)).isReturned()
            .when(requestingPoints().withDistance(days).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then(percentile().as(lessThanOrEqualTo(15).forMeasurement("m3")).fromSeriesWithDistance(days)).isReturned()
            .when(requestingPoints().withDistance(days).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then(percentile().as(greaterThanOrEqualTo(57).forMeasurement("m4")).fromSeriesWithDistance(days)).isReturned();
    }

    @Test
    public void givenMonthSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        given(aSeries().withDistance(months).withMeasurements("m1", "m2", "m3", "m4"))
            .when(requestingPoints().withDistance(months).withMeasurement("m1").lessThanPercentile(92))
                .then(percentile().as(lessThan(92).forMeasurement("m1"))).isReturned()
            .when(requestingPoints().withDistance(months).withMeasurement("m2").greaterThanPercentile(43))
                .then(percentile().as(greaterThan(43).forMeasurement("m2"))).isReturned()
            .when(requestingPoints().withDistance(months).withMeasurement("m3").lessThanOrEqualToPercentile(15))
                .then(percentile().as(lessThanOrEqualTo(15).forMeasurement("m3"))).isReturned()
            .when(requestingPoints().withDistance(months).withMeasurement("m4").greaterThanOrEqualToPercentile(57))
                .then(percentile().as(greaterThanOrEqualTo(57).forMeasurement("m4"))).isReturned();
    }

    @Test
    public void givenSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        Given given = given(
                aSeries().withDistance(minutes),
                aSeries().withDistance(hours),
                aSeries().withDistance(days),
                aSeries().withDistance(months),
                aSeries().withDistance(years)
        );
        forEachMeasurementDistance(distance -> given
                .when(requestingPoints().lessThanPercentile(92).withDistance(distance).withMeasurement("m1"))
                .then(percentile().as(lessThan(92).forMeasurement("m1")).fromSeriesWithDistance(distance)).isReturned()
                .when(requestingPoints().greaterThanPercentile(43).withDistance(distance).withMeasurement("m2"))
                .then(percentile().as(greaterThan(43).forMeasurement("m2")).fromSeriesWithDistance(distance)).isReturned()
                .when(requestingPoints().lessThanOrEqualToPercentile(15).withDistance(distance).withMeasurement("m3"))
                .then(percentile().as(lessThanOrEqualTo(15).forMeasurement("m3")).fromSeriesWithDistance(distance)).isReturned()
                .when(requestingPoints().greaterThanOrEqualToPercentile(57).withDistance(distance).withMeasurement("m4"))
                .then(percentile().as(greaterThanOrEqualTo(57).forMeasurement("m4")).fromSeriesWithDistance(distance)).isReturned());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthPointsThenSummarizedMinutesAreReturned() {
        given(aSeries().withDistance(minutes))
            .when(requestingPoints().withDistance(months))
                .then(sumPer(months).withDistance(minutes)).isReturned();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForLastPerMeasurementDistanceThenLastPointPerMeasurementDistanceIsReturned() {
        given(aSeries().withDistance(minutes)/*.withCategories("a=b", "c=d", "e=f").withSize(5)*/)
                .when(requestingPoints().withDistance(minutes).withLastPer(minutes))
                .then(lastPoint().per(minutes)).isReturned()
                .when(requestingPoints().withDistance(minutes).withLastPer(hours))
                .then(lastPoint().per(hours)).isReturned()
                .when(requestingPoints().withDistance(minutes).withLastPer(days))
                .then(lastPoint().per(days)).isReturned()
                .when(requestingPoints().withDistance(minutes).withLastPer(months))
                .then(lastPoint().per(months)).isReturned()
                .when(requestingPoints().withDistance(minutes).withLastPer(years))
                .then(lastPoint().per(years)).isReturned();
    }

    @Test
    public void givenSeriesWhenQueryingForLastPerMeasurementDistanceThenLastPointPerMeasurementDistanceIsReturned() {
        Given given = given(
                aSeries().withDistance(minutes),
                aSeries().withDistance(hours),
                aSeries().withDistance(days),
                aSeries().withDistance(months),
                aSeries().withDistance(years)
        );
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::lessThanOrEqualTo, targetDistance ->
                        given.when(requestingPoints().withLastPer(targetDistance).withDistance(distance))
                                .then(lastPoint().per(targetDistance).withDistance(distance)).isReturned()));
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::greaterThan, targetDistance ->
                        given.when(requestingPoints().withLastPer(targetDistance).withDistance(distance))
                                .then(null).requestFails(
                                format("500/Distance %s is greater than target distance %s", distance, targetDistance))));
    }

    @Test
    public void givenSeriesWhenQueryingForSumPerMeasurementDistanceThenSumPointPerMeasurementDistanceIsReturned() {
        Given givenSomeSeries = given(
                aSeries().withDistance(minutes).withCategories("a=b", "c=d", "e=f"),
                aSeries().withDistance(hours).withCategories("a=b", "c=d", "e=f"),
                aSeries().withDistance(days).withCategories("a=b", "c=d", "e=f"),
                aSeries().withDistance(months).withCategories("a=b", "c=d", "e=f"),
                aSeries().withDistance(years).withCategories("a=b", "c=d", "e=f")
        );
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::lessThanOrEqualTo, targetDistance ->
                        givenSomeSeries.when(requestingPoints().withSumPer(targetDistance).withCategory("a=b").withDistance(distance))
                                .then(sumPer(targetDistance).withCategory("a", "b").withDistance(distance)).isReturned()));
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::greaterThan, targetDistance ->
                        givenSomeSeries.when(requestingPoints().withLastPer(targetDistance).withDistance(distance))
                                .then(null).requestFails(
                                format("500/Distance %s is greater than target distance %s", distance, targetDistance))));
    }

    @Test
    public void givenMinuteSeriesOverMoreThanOneMonthWhenQueryingForMonthSnapshotsThenLastPointInEveryMonthAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 100, "measurementA", "measurementB");
        helper.indexPoints(minutes, points);
        List<TimeSeriesPoint> pointsMonth2 = createRandomTimeSeries(now.truncatedTo(DAYS).plusMonths(1), minutes, 100, "measurementA", "measurementB");
        helper.indexPoints(minutes, pointsMonth2);

        List<TimeSeriesPoint> resultingPoints = requestingPoints().from(series).withDistance(minutes)
                .from(now.truncatedTo(DAYS))
                .to(now.truncatedTo(DAYS).plusMonths(1).plusMinutes(100))
                .withLastPer(months).get();

        assertEquals(2, size(resultingPoints));

        assertEquals(value(99, "measurementA", points), value(0, "measurementA", resultingPoints));
        assertEquals(value(99, "measurementB", points), value(0, "measurementB", resultingPoints));
        assertEquals(value(99, "measurementA", pointsMonth2), value(1, "measurementA", resultingPoints));
        assertEquals(value(99, "measurementB", pointsMonth2), value(1, "measurementB", resultingPoints));

        assertEquals(truncatedTimestamp(now, MONTHS).toInstant(), timestamp(0, resultingPoints).toInstant());
        assertEquals(truncatedTimestamp(now.plusMonths(1), MONTHS).toInstant(), timestamp(1, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForDayPointsThenSummarizedMinutesAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 100, "measurementA", "measurementB");
        helper.indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = request(
                days,
                series,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(100)
        );
        assertEquals(1, size(resultingPoints));
        assertEquals(DataOperations.sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(truncatedTimestamp(now, ChronoUnit.DAYS).toInstant(), timestamp(0, resultingPoints).toInstant());
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForDayPointsForSeveralDaysThenSummarizedMinutesForEachDayAreReturned() throws IOException {
        List<TimeSeriesPoint> points = createRandomTimeSeries(now.truncatedTo(DAYS), minutes, 2000, "measurementA", "measurementB");
        helper.indexPoints(minutes, points);
        List<TimeSeriesPoint> resultingPoints = request(
                days,
                series,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plusMinutes(2000)
        );

        List<TimeSeriesPoint> pointsDayOne = points.subList(0,1440);
        List<TimeSeriesPoint> pointsDayTwo = points.subList(1440, points.size());
        assertEquals(2, size(resultingPoints));
        assertEquals(DataOperations.sum("measurementA", pointsDayOne), resultingPoints.get(0).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementA", pointsDayTwo), resultingPoints.get(1).getMeasurement("measurementA").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", pointsDayOne), resultingPoints.get(0).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", pointsDayTwo), resultingPoints.get(1).getMeasurement("measurementB").map(Measurement::getValue).orElse(-1L).longValue());

        assertEquals(DataOperations.sum("measurementA", points), DataOperations.sum("measurementA", resultingPoints));
        assertEquals(DataOperations.sum("measurementB", points), DataOperations.sum("measurementB", resultingPoints));
        assertEquals(truncatedTimestamp(now, ChronoUnit.DAYS).toInstant(), timestamp(0, resultingPoints).toInstant());
        assertEquals(truncatedTimestamp(now.plusDays(1), ChronoUnit.DAYS).toInstant(), timestamp(1, resultingPoints).toInstant());
    }

    @Test
    public void givenSeriesWhenRequestingUnboundedSumThenSingleSummarizedPointIsReturned() {
        Given given = given(
                aSeries().withDistance(minutes),
                aSeries().withDistance(hours),
                aSeries().withDistance(days),
                aSeries().withDistance(months),
                aSeries().withDistance(years)
        );
        for (MeasurementDistance distance : MeasurementDistance.values()) {
            given.when(requestingPoints().sumPoint().withDistance(distance))
                    .then(sum().withDistance(distance)).isReturned();
        }
    }

    @Test
    public void givenSeriesWhenRequestingLeftUnboundedSumThenSingleSummarizedPointIsReturned() {
        Given given = given(
                aSeries().withDistance(minutes),
                aSeries().withDistance(hours),
                aSeries().withDistance(days),
                aSeries().withDistance(months),
                aSeries().withDistance(years)
        );
        ZonedDateTime timestampOfLastPoint = ZonedDateTime.now();
        for (MeasurementDistance distance : MeasurementDistance.values()) {
            given.when(requestingPoints().sumPoint().withDistance(distance).to(timestampOfLastPoint))
                    .then(sum().withDistance(distance).to(timestampOfLastPoint)).isReturned();
        }
    }

    @Test
    public void givenSeriesWhenRequestingRightUnboundedSumThenSingleSummarizedPointIsReturned() {
        Given givenExistingSeries= given(
                aSeries().withDistance(minutes),
                aSeries().withDistance(hours),
                aSeries().withDistance(days),
                aSeries().withDistance(months),
                aSeries().withDistance(years)
        );
        ZonedDateTime timestampOfFirstPoint = ZonedDateTime.now().minusYears(200);
        for (MeasurementDistance distance : MeasurementDistance.values()) {
            givenExistingSeries.when(requestingPoints().sumPoint().withDistance(distance).from(timestampOfFirstPoint))
                    .then(sum().withDistance(distance).from(timestampOfFirstPoint)).isReturned();
        }
    }

    @Test
    public void givenFixedMinuteSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(minutes);
    }

    @Test
    public void givenFixedHourSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(hours);
    }

    @Test
    public void givenFixedDaySeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(days);
    }

    @Test
    public void givenFixedMonthSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(months);
    }

    @Test
    public void givenFixedYearSeriesWhenRequestingSumThenSingleSummarizedPointIsReturned() throws IOException {
        givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(years);
    }

    private void givenFixedSeriesWhenRequestingSumThenSumPointIsReturned(MeasurementDistance distance) throws IOException {
        // Note that sum which gives larger numbers than 2^52 will be inaccurate, as Elasticsearch uses doubles to avoid
        // overflow.
        List<TimeSeriesPoint> points = helper.indexPointsFrom(now.truncatedTo(DAYS), distance, (long)Math.pow(2, 51), (long)Math.pow(2, 51)+121);
        TimeSeriesPoint resultingPoint = requestSum(
                series,
                distance,
                now.truncatedTo(DAYS),
                now.truncatedTo(DAYS).plus(100, unit(distance))
        );
        assertNotNull(resultingPoint);
        assertEquals(DataOperations.sum(measurementId, points), measurementValue(measurementId, resultingPoint));
        assertEquals(now.truncatedTo(DAYS).plus(1, unit(distance)).toInstant(), resultingPoint.getTimestamp().toInstant());
    }

    private void forEachMeasurementDistance(Consumer<MeasurementDistance> action) {
        Arrays.stream(MeasurementDistance.values()).forEach(action);
    }

    private void forEachMeasurementDistance(Predicate<MeasurementDistance> filter,  Consumer<MeasurementDistance> action) {
        Arrays.stream(MeasurementDistance.values()).filter(filter).forEach(action);
    }

    private TimeSeriesPoint requestLast(String seriesName, MeasurementDistance distance, String owner, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}/last?from={from}&to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(TimeSeriesPoint.class).readValue(response.getBody());
    }

    private TimeSeriesPoint requestLast(String seriesName, MeasurementDistance distance, String owner) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}/last",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(TimeSeriesPoint.class).readValue(response.getBody());
    }

    private TimeSeriesPoint requestSum(String seriesName, MeasurementDistance distance, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}/sum?from={from}&to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(TimeSeriesPoint.class).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> request(MeasurementDistance distance, String seriesName, ZonedDateTime from, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}?from={from}&to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(from),
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> requestTo(MeasurementDistance distance, String seriesName, ZonedDateTime to) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}?to={to}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(to)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesPoint> requestFrom(MeasurementDistance distance, String seriesName, ZonedDateTime from) throws IOException {
        ResponseEntity<String> response = restTemplate.exchange(
                "/{owner}/{seriesName}/{distance}?from={from}",
                HttpMethod.GET,
                null,
                String.class,
                owner,
                seriesName,
                distance,
                formatTimestamp(from)
        );
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readerFor(new TypeReference<List<TimeSeriesPoint>>(){}).readValue(response.getBody());
    }

    private List<TimeSeriesDefinition> requestingAvailableTimeSeries() {
        ResponseEntity<String> response = restTemplate.exchange(
                "/meta",
                HttpMethod.GET,
                null,
                String.class
        );
        assertEquals(200, response.getStatusCodeValue());
        try {
            return objectMapper.readerFor(new TypeReference<List<TimeSeriesDefinition>>(){}).readValue(response.getBody());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private WhenSupplier requestingPoints() {
        return new WhenSupplier(restTemplate, objectMapper);
    }

    private TimeSeriesGenerator aSeries() {
        return new TimeSeriesGenerator(helper);
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

    private static ZonedDateTime dateTime(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, UTC);
    }

    public static class PercentileFilterBuilder implements Function<TimeSeries, List<TimeSeriesPoint>> {

        private RelationalOperator operator;
        private int percentile;
        private String measurementId;

        PercentileFilterBuilder(RelationalOperator operator) {
            this.operator = operator;
        }

        static PercentileFilterBuilder lessThan(int percentile) {
            return new PercentileFilterBuilder(lt).percentile(percentile);
        }

        static PercentileFilterBuilder greaterThan(int percentile) {
            return new PercentileFilterBuilder(gt).percentile(percentile);
        }

        static PercentileFilterBuilder lessThanOrEqualTo(int percentile) {
            return new PercentileFilterBuilder(lte).percentile(percentile);
        }

        static PercentileFilterBuilder greaterThanOrEqualTo(int percentile) {
            return new PercentileFilterBuilder(gte).percentile(percentile);
        }

        PercentileFilterBuilder percentile(int percentile) {
            this.percentile = percentile;
            return this;
        }

        PercentileFilterBuilder forMeasurement(String measurementId) {
            this.measurementId = measurementId;
            return this;
        }

        @Override
        public List<TimeSeriesPoint> apply(TimeSeries series) {
            return relativeToPercentile(operator, measurementId, percentile).apply(series);
        }

    }

}
