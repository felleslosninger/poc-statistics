package no.difi.statistics;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.difi.statistics.elasticsearch.Client;
import no.difi.statistics.model.MeasurementDistance;
import no.difi.statistics.model.TimeSeriesPoint;
import no.difi.statistics.query.config.AppConfig;
import no.difi.statistics.query.elasticsearch.config.ElasticsearchConfig;
import no.difi.statistics.query.elasticsearch.helpers.*;
import no.difi.statistics.test.utils.DataOperations;
import no.difi.statistics.test.utils.ElasticsearchHelper;
import no.difi.statistics.test.utils.ElasticsearchRule;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.test.web.client.LocalHostUriTemplateHandler;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.util.DefaultUriBuilderFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.DAYS;
import static no.difi.statistics.elasticsearch.Timestamp.truncatedTimestamp;
import static no.difi.statistics.model.MeasurementDistance.*;
import static no.difi.statistics.query.elasticsearch.helpers.AvailableSeriesQuery.calculatedAvailableSeries;
import static no.difi.statistics.query.elasticsearch.helpers.AvailableSeriesQuery.requestingAvailableTimeSeries;
import static no.difi.statistics.query.elasticsearch.helpers.LastHistogramQuery.requestingLastHistogram;
import static no.difi.statistics.query.elasticsearch.helpers.LastQuery.requestingLast;
import static no.difi.statistics.query.elasticsearch.helpers.PercentileQuery.requestingPercentile;
import static no.difi.statistics.query.elasticsearch.helpers.SumHistogramQuery.calculatedSumHistogram;
import static no.difi.statistics.query.elasticsearch.helpers.SumHistogramQuery.requestingSumHistogram;
import static no.difi.statistics.query.elasticsearch.helpers.SumQuery.requestingSum;
import static no.difi.statistics.query.elasticsearch.helpers.TimeSeriesQuery.requestingSeries;
import static no.difi.statistics.query.elasticsearch.helpers.TimeSeriesQuery.withAttributes;
import static no.difi.statistics.query.elasticsearch.helpers.Verification.given;
import static no.difi.statistics.test.utils.DataGenerator.createRandomTimeSeries;
import static no.difi.statistics.test.utils.DataOperations.*;
import static org.junit.Assert.assertEquals;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = {AppConfig.class, ElasticsearchConfig.class}, initializers = ElasticsearchQueryServiceTest.Initializer.class)
@RunWith(SpringRunner.class)
@TestPropertySource(properties = {"file.base.difi-statistikk=src/test/resources/apikey"})
public class ElasticsearchQueryServiceTest {

    @ClassRule
    public static ElasticsearchRule elasticsearchRule = new ElasticsearchRule();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertyValues.of(
                    "no.difi.statistics.elasticsearch.host=" + elasticsearchRule.getHost(),
                    "no.difi.statistics.elasticsearch.port=" + elasticsearchRule.getPort()
            ).applyTo(applicationContext);
        }

    }

    private final static ZoneId UTC = ZoneId.of("UTC");
    private ZonedDateTime now = ZonedDateTime.of(2016, 3, 3, 0, 0, 0, 0, UTC);
    private ZonedDateTime startOf2017 = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, UTC);
    private ZonedDateTime startOf2018 = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, UTC);
    private ZonedDateTime startOfOctober2017 = ZonedDateTime.of(2017, 10, 1, 0, 0, 0, 0, UTC);
    private ZonedDateTime startOfNovember2017 = ZonedDateTime.of(2017, 11, 1, 0, 0, 0, 0, UTC);
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
    @Autowired
    private Environment environment;

    @Before
    public void prepare() {
        helper = new ElasticsearchHelper(client);
        helper.waitForGreenStatus();
        // Need to change encoding mode on the client, as it does not encode '+' per default (since Spring Framework 5.0/Spring Boot 2.0).
        // We need this for 'from' and 'to' query parameters. Unfortunately there doesn't seem to be a clean way of
        // specifying encoding mode on the TestRestTemplate.
        DefaultUriBuilderFactory f = new DefaultUriBuilderFactory();
        f.setEncodingMode(DefaultUriBuilderFactory.EncodingMode.VALUES_ONLY);
        LocalHostUriTemplateHandler uth = new LocalHostUriTemplateHandler(environment, "http", f);
        restTemplate.getRestTemplate().setUriTemplateHandler(uth);
        // Quick'n'dirty, could be improved
        QueryClient.restTemplate = restTemplate;
        QueryClient.objectMapper = objectMapper;
    }

    @After
    public void cleanup() {
        helper.clear();
    }

    @Test
    public void givenTimeSeriesWhenQueryingForAvailableTimeSeriesThenAvailableTimeSeriesAreReturned() {
        given(
                aSeries(withAttributes().distance(minutes).name("series1").owner("owner1")),
                aSeries(withAttributes().distance(hours).name("series1").owner("owner2")),
                aSeries(withAttributes().distance(days).name("series1").owner("owner3")),
                aSeries(withAttributes().distance(months).name("series1").owner("owner4")),
                aSeries(withAttributes().distance(years).name("series1").owner("owner5"))
        )
                .when(requestingAvailableTimeSeries())
                .thenIsReturned(calculatedAvailableSeries());
    }

    @Test
    public void givenCategorizedSeriesWhenQueryingForLastPointThenThatIsReturned() {
        given(aSeries(withAttributes().distance(minutes)).category("a", "b").category("a", "c").category("b", "c"))
            .when(requestingLast().distance(minutes))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenCategorizedSeriesWhenQueryingForLastPointWithASpecificCategoryThenThatIsReturned() {
        given(aSeries(withAttributes().distance(minutes)).category("a", "b").category("a", "c").category("b", "c"))
                .when(requestingLast().distance(minutes).category("a", "b"))
                .thenThatSeriesIsReturned();
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
        ZonedDateTime rangeStart = dateTime(2007, 1, 1, 0, 0);
        ZonedDateTime rangeEnd = dateTime(2016, 3, 3, 13, 0);
        ZonedDateTime lastTimeWithinRange = dateTime(2016, 3, 3, 12, 12);
        ZonedDateTime lastTimeAfterRange = dateTime(2017, 4, 4, 1, 2);
        helper.indexPointsFrom(dateTime(2003, 1, 1, 11, 11), distance, 1, 2, 3, 4, 5, 6, 7, 8, 9); // Some random "old" points
        TimeSeriesPoint expectedLastPoint = helper.indexPoint(distance, lastTimeWithinRange, 123L);
        helper.indexPoint(distance, lastTimeAfterRange, 5675L);
        TimeSeriesPoint actualLastPoint = requestLast(series, distance, owner, rangeStart, rangeEnd);
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
    public void givenMinuteSeriesWhenQueryingWithoutRangeThenAllDataPointsAreReturned() {
        given(aSeries(withAttributes().distance(minutes)))
                .when(requestingSeries().distance(minutes))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithCategoryThenPointsWithThatCategoryAreReturned() {
        given(aSeries(withAttributes().distance(minutes)).category("Category A", "Value for category A").category("Category B", "Value for category B"))
                .when(requestingSeries().distance(minutes).category("Category A", "Value for category A"))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenDaySeriesWhenQueryingWithCategoryAndClosedRangeThenPointsWithThatCategoryWithinTheRangeAreReturned() {
        given(aSeries(withAttributes().distance(days).from(startOf2017).to(startOf2018)).category("TL", "Bergen kommune").category("TL", "Alvdal kommune"))
                .when(requestingSeries().distance(days).from(startOfOctober2017).to(startOfNovember2017).category("TL","Bergen kommune"))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenMinuteSeriesWhenQueryingWithoutCategoryThenPointsWithSummarizedCategoriesAreReturned() {
        given(aSeries(withAttributes().distance(minutes)).category("a", "b").category("c", "d"))
                .when(requestingSeries().distance(minutes))
                .thenThatSeriesIsReturned();
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
        given(aSeries(withAttributes().distance(minutes)).withMeasurements("m1", "m2", "m3", "m4"))
            .when(requestingPercentile().lessThan(92).withMeasurement("m1").distance(minutes))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().greaterThan(43).withMeasurement("m2").distance(minutes))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().lessThanOrEqualTo(15).withMeasurement("m3").distance(minutes))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().greaterThanOrEqualTo(57).withMeasurement("m4").distance(minutes))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenHourSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        given(aSeries(withAttributes().distance(hours)).withMeasurements("m1", "m2", "m3", "m4"))
            .when(requestingPercentile().lessThan(92).withMeasurement("m1").distance(hours))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().greaterThan(43).withMeasurement("m2").distance(hours))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().lessThanOrEqualTo(15).withMeasurement("m3").distance(hours))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().greaterThanOrEqualTo(57).withMeasurement("m4").distance(hours))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenDaySeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        given(aSeries(withAttributes().distance(days)).withMeasurements("m1", "m2", "m3", "m4"))
            .when(requestingPercentile().lessThan(92).withMeasurement("m1").distance(days))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().greaterThan(43).withMeasurement("m2").distance(days))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().lessThanOrEqualTo(15).withMeasurement("m3").distance(days))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().greaterThanOrEqualTo(57).withMeasurement("m4").distance(days))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenMonthSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        given(aSeries(withAttributes().distance(months)).withMeasurements("m1", "m2", "m3", "m4"))
            .when(requestingPercentile().lessThan(92).withMeasurement("m1").distance(months))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().greaterThan(43).withMeasurement("m2").distance(months))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().lessThanOrEqualTo(15).withMeasurement("m3").distance(months))
                .thenThatSeriesIsReturned()
            .when(requestingPercentile().greaterThanOrEqualTo(57).withMeasurement("m4").distance(months))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenSeriesWhenRequestingPointsWithAMeasurementRelativeToAPercentileThenThosePointsAreReturned() {
        Verification.WhenStep given = given(
                aSeries(withAttributes().distance(minutes)),
                aSeries(withAttributes().distance(hours)),
                aSeries(withAttributes().distance(days)),
                aSeries(withAttributes().distance(months)),
                aSeries(withAttributes().distance(years))
        );
        forEachMeasurementDistance(distance -> given
                .when(requestingPercentile().lessThan(92).withMeasurement("m1").distance(distance))
                .thenThatSeriesIsReturned()
                .when(requestingPercentile().greaterThan(43).withMeasurement("m2").distance(distance))
                .thenThatSeriesIsReturned()
                .when(requestingPercentile().lessThanOrEqualTo(15).withMeasurement("m3").distance(distance))
                .thenThatSeriesIsReturned()
                .when(requestingPercentile().greaterThanOrEqualTo(57).withMeasurement("m4").distance(distance))
                .thenThatSeriesIsReturned()
        );
    }

    @Test
    public void givenMinuteSeriesWhenQueryingForMonthPointsThenSummarizedMinutesAreReturned() {
        given(aSeries(withAttributes().distance(minutes)))
            .when(requestingSeries().distance(months))
                .thenIsReturned(calculatedSumHistogram().per(months).distance(minutes));
    }

    @Test
    public void givenCategorizedSeriesWhenQueryingForLastPerMeasurementDistanceThenThatSeriesIsReturned() {
        Verification.WhenStep given = given(
                aSeries(withAttributes().distance(minutes)).category("a", "b").category("a", "c").category("d", "e"),
                aSeries(withAttributes().distance(hours)).category("a", "b").category("a", "c").category("d", "e"),
                aSeries(withAttributes().distance(days)).category("a", "b").category("a", "c").category("d", "e"),
                aSeries(withAttributes().distance(months)).category("a", "b").category("a", "c").category("d", "e"),
                aSeries(withAttributes().distance(years)).category("a", "b").category("a", "c").category("d", "e")
        );
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::lessThan, targetDistance ->
                        given.when(requestingLastHistogram().per(targetDistance).distance(distance))
                                .thenThatSeriesIsReturned()));
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::greaterThanOrEqualTo, targetDistance ->
                        given.when(requestingLastHistogram().per(targetDistance).distance(distance))
                                .thenFailsWithMessage(
                                        format("500/Distance %s is greater than or equal to target distance %s", distance, targetDistance)
                                )
                )
        );
    }

    @Test
    public void givenCategorizedSeriesWhenQueryingForLastPerMeasurementDistanceForACategoryThenThatSeriesIsReturned() {
        Verification.WhenStep given = given(
                aSeries(withAttributes().distance(minutes)).category("a", "b").category("a", "c").category("d", "e"),
                aSeries(withAttributes().distance(hours)).category("a", "b").category("a", "c").category("d", "e"),
                aSeries(withAttributes().distance(days)).category("a", "b").category("a", "c").category("d", "e"),
                aSeries(withAttributes().distance(months)).category("a", "b").category("a", "c").category("d", "e"),
                aSeries(withAttributes().distance(years)).category("a", "b").category("a", "c").category("d", "e")
        );
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::lessThan, targetDistance ->
                        given.when(requestingLastHistogram().per(targetDistance).distance(distance).category("a","c"))
                                .thenThatSeriesIsReturned()));
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::greaterThanOrEqualTo, targetDistance ->
                        given.when(requestingLastHistogram().per(targetDistance).distance(distance).category("a", "c"))
                                .thenFailsWithMessage(
                                        format("500/Distance %s is greater than or equal to target distance %s", distance, targetDistance)
                                )
                )
        );
    }

    @Test
    public void givenSeriesWhenQueryingForLastPerMeasurementDistanceThenLastPointPerMeasurementDistanceIsReturned() {
        Verification.WhenStep given = given(
                aSeries(withAttributes().distance(minutes)),
                aSeries(withAttributes().distance(hours)),
                aSeries(withAttributes().distance(days)),
                aSeries(withAttributes().distance(months)),
                aSeries(withAttributes().distance(years))
        );
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::lessThan, targetDistance ->
                        given.when(requestingLastHistogram().per(targetDistance).distance(distance))
                                .thenThatSeriesIsReturned()));
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::greaterThanOrEqualTo, targetDistance ->
                        given.when(requestingLastHistogram().per(targetDistance).distance(distance))
                                .thenFailsWithMessage(
                                        format("500/Distance %s is greater than or equal to target distance %s", distance, targetDistance)
                                )
                )
        );
    }

    @Test
    public void givenSeriesWhenQueryingForSumPerMeasurementDistanceThenSumPointPerMeasurementDistanceIsReturned() {
        Verification.WhenStep givenSomeSeries = given(
                aSeries(withAttributes().distance(minutes)).category("a", "b").category("c", "d").category("e", "f"),
                aSeries(withAttributes().distance(hours)).category("a", "b").category("c", "d").category("e", "f"),
                aSeries(withAttributes().distance(days)).category("a", "b").category("c", "d").category("e", "f"),
                aSeries(withAttributes().distance(months)).category("a", "b").category("c", "d").category("e", "f"),
                aSeries(withAttributes().distance(years)).category("a", "b").category("c", "d").category("e", "f")
        );
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::lessThan, targetDistance ->
                        givenSomeSeries.when(requestingSumHistogram().per(targetDistance).category("a", "b").distance(distance))
                                .thenThatSeriesIsReturned()));
        forEachMeasurementDistance(distance ->
                forEachMeasurementDistance(distance::greaterThanOrEqualTo, targetDistance ->
                        givenSomeSeries.when(requestingSumHistogram().per(targetDistance).distance(distance))
                                .thenFailsWithMessage(
                                        format("500/Distance %s is greater than or equal to target distance %s", distance, targetDistance)
                                )
                )
        );
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
        assertEquals(DataOperations.sum("measurementA", points), resultingPoints.get(0).getMeasurement("measurementA").orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", points), resultingPoints.get(0).getMeasurement("measurementB").orElse(-1L).longValue());
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
        assertEquals(DataOperations.sum("measurementA", pointsDayOne), resultingPoints.get(0).getMeasurement("measurementA").orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementA", pointsDayTwo), resultingPoints.get(1).getMeasurement("measurementA").orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", pointsDayOne), resultingPoints.get(0).getMeasurement("measurementB").orElse(-1L).longValue());
        assertEquals(DataOperations.sum("measurementB", pointsDayTwo), resultingPoints.get(1).getMeasurement("measurementB").orElse(-1L).longValue());

        assertEquals(DataOperations.sum("measurementA", points), DataOperations.sum("measurementA", resultingPoints));
        assertEquals(DataOperations.sum("measurementB", points), DataOperations.sum("measurementB", resultingPoints));
        assertEquals(truncatedTimestamp(now, ChronoUnit.DAYS).toInstant(), timestamp(0, resultingPoints).toInstant());
        assertEquals(truncatedTimestamp(now.plusDays(1), ChronoUnit.DAYS).toInstant(), timestamp(1, resultingPoints).toInstant());
    }

    @Test
    public void givenSeriesWhenRequestingUnboundedSumThenSingleSummarizedPointIsReturned() {
        Verification.WhenStep given = given(
                aSeries(withAttributes().distance(minutes)),
                aSeries(withAttributes().distance(hours)),
                aSeries(withAttributes().distance(days)),
                aSeries(withAttributes().distance(months)),
                aSeries(withAttributes().distance(years))
        );
        for (MeasurementDistance distance : MeasurementDistance.values()) {
            given.when(requestingSum().distance(distance))
                    .thenThatSeriesIsReturned();
        }
    }

    @Test
    public void givenSeriesWhenRequestingLeftUnboundedSumThenSingleSummarizedPointIsReturned() {
        Verification.WhenStep given = given(
                aSeries(withAttributes().distance(minutes)),
                aSeries(withAttributes().distance(hours)),
                aSeries(withAttributes().distance(days)),
                aSeries(withAttributes().distance(months)),
                aSeries(withAttributes().distance(years))
        );
        ZonedDateTime timestampOfLastPoint = ZonedDateTime.now();
        for (MeasurementDistance distance : MeasurementDistance.values()) {
            given.when(requestingSum().distance(distance).to(timestampOfLastPoint))
                    .thenThatSeriesIsReturned();
        }
    }

    @Test
    public void givenSeriesWhenRequestingRightUnboundedSumThenSingleSummarizedPointIsReturned() {
        Verification.WhenStep givenExistingSeries= given(
                aSeries(withAttributes().distance(minutes)),
                aSeries(withAttributes().distance(hours)),
                aSeries(withAttributes().distance(days)),
                aSeries(withAttributes().distance(months)),
                aSeries(withAttributes().distance(years))
        );
        ZonedDateTime timestampOfFirstPoint = ZonedDateTime.now().minusYears(200);
        for (MeasurementDistance distance : MeasurementDistance.values()) {
            givenExistingSeries.when(requestingSum().distance(distance).from(timestampOfFirstPoint))
                    .thenThatSeriesIsReturned();
        }
    }

    @Test
    public void givenSeriesWhenRequestingSumThenSumPointIsReturned() {
        given(
                aSeries(withAttributes().distance(minutes)).category("a", "b").category("c", "d"),
                aSeries(withAttributes().distance(hours)).category("a", "b").category("c", "d"),
                aSeries(withAttributes().distance(days)).category("a", "b").category("c", "d"),
                aSeries(withAttributes().distance(months)).category("a", "b").category("c", "d"),
                aSeries(withAttributes().distance(years)).category("a", "b").category("c", "d")
        )
                .when(requestingSum().distance(minutes).category("a", "b"))
                .thenThatSeriesIsReturned()
                .when(requestingSum().distance(hours).category("a", "b"))
                .thenThatSeriesIsReturned()
                .when(requestingSum().distance(days).category("a", "b"))
                .thenThatSeriesIsReturned()
                .when(requestingSum().distance(months).category("a", "b"))
                .thenThatSeriesIsReturned()
                .when(requestingSum().distance(years).category("a", "b"))
                .thenThatSeriesIsReturned();
    }

    @Test
    public void givenSeriesWhenRequestingPerCategoryThenCorrectSubsetIsReturned() {
        given(
                aSeries(withAttributes().distance(hours)).category("a", "b").category("a", "c").category("d", "e")
        )
                .when(requestingSeries().distance(hours).perCategory("a"))
                .thenThatSeriesIsReturned();
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
        assertEquals(response.getBody(), 200, response.getStatusCodeValue());
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

    private TimeSeriesGenerator aSeries(TimeSeriesQuery attributes) {
        return new TimeSeriesGenerator(helper).withAttributes(attributes);
    }

    private String formatTimestamp(ZonedDateTime timestamp) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp);
    }

    private static ZonedDateTime dateTime(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, UTC);
    }

}
