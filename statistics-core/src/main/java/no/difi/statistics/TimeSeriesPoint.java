package no.difi.statistics;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.time.ZonedDateTime;

@XmlRootElement
public class TimeSeriesPoint {

    private ZonedDateTime time;
    private int value;

    public TimeSeriesPoint(ZonedDateTime time, int value) {
        this.time = time;
        this.value = value;
    }

    @XmlElement
    public ZonedDateTime getTime() {
        return time;
    }

    @XmlElement
    public int getValue() {
        return value;
    }

}
