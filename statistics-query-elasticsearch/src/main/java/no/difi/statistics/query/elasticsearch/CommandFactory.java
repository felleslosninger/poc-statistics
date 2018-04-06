package no.difi.statistics.query.elasticsearch;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class CommandFactory implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public GetLastHistogram.Builder lastHistogram() {
        return applicationContext.getBean(GetLastHistogram.Builder.class);
    }

    public GetSumHistogram.Builder sumHistogram() {
        return applicationContext.getBean(GetSumHistogram.Builder.class);
    }

    public GetMeasurementIdentifiers.Builder measurementIdentifiers() {
        return applicationContext.getBean(GetMeasurementIdentifiers.Builder.class);
    }

    public GetAvailableTimeSeries.Builder availableTimeSeries() {
        return applicationContext.getBean(GetAvailableTimeSeries.Builder.class);
    }

}
