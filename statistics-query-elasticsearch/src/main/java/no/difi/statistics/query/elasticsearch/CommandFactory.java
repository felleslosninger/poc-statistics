package no.difi.statistics.query.elasticsearch;

import no.difi.statistics.query.elasticsearch.commands.*;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class CommandFactory implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public TimeSeriesQuery.Builder query() {
        return applicationContext.getBean(TimeSeriesQuery.Builder.class);
    }

    public LastHistogramQuery.Builder lastHistogram() {
        return applicationContext.getBean(LastHistogramQuery.Builder.class);
    }

    public LastQuery.Builder last() {
        return applicationContext.getBean(LastQuery.Builder.class);
    }

    public SumHistogramQuery.Builder sumHistogram() {
        return applicationContext.getBean(SumHistogramQuery.Builder.class);
    }

    public SumQuery.Builder sum() {
        return applicationContext.getBean(SumQuery.Builder.class);
    }

    public PercentileQuery.Builder percentile() {
        return applicationContext.getBean(PercentileQuery.Builder.class);
    }

    public GetMeasurementIdentifiers.Builder measurementIdentifiers() {
        return applicationContext.getBean(GetMeasurementIdentifiers.Builder.class);
    }

    public AvailableSeriesQuery.Builder availableTimeSeries() {
        return applicationContext.getBean(AvailableSeriesQuery.Builder.class);
    }

}
