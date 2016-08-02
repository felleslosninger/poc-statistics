package no.difi.statistics.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.difi.statistics.client.exception.DifiStatisticsException;
import no.difi.statistics.client.model.Measurement;
import no.difi.statistics.client.model.TimeSeriesPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonHelper {

    private HttpHelper httpHelper;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public JsonHelper(final HttpHelper httpHelper){
        this.httpHelper = httpHelper;
    }


    protected OutputStream writeJsonToOutPutStream(Map<String, Integer> measurements, ZonedDateTime timestamp, HttpURLConnection conn)throws DifiStatisticsException {
        TimeSeriesPoint timeSeriesPoint = makeTimeSeriesPoint(measurements, timestamp);
        String jsonString = createValidJsonString(timeSeriesPoint);
        OutputStream outputStream = httpHelper.getOutputStream(conn);
        writeJsonString(outputStream, jsonString);

        return outputStream;
    }

    private TimeSeriesPoint makeTimeSeriesPoint(Map<String, Integer> measurementsMap, ZonedDateTime timestamp) {
        List<Measurement> measurememts = new ArrayList<>();
        measurementsMap.keySet()
                .stream()
                .forEach(id -> measurememts.add(new Measurement(id, measurementsMap.get(id))));
        return TimeSeriesPoint.builder().measurements(measurememts).timestamp(timestamp).build();
    }

    private String createValidJsonString(TimeSeriesPoint timeSeriesPoint) throws DifiStatisticsException{
        String json = "";

        try {
            json = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .writeValueAsString(timeSeriesPoint);
        }catch (JsonProcessingException e){
            throw new DifiStatisticsException("Could not create json string", e);
        }
        if (json.equals("")) {
            throw new DifiStatisticsException("Could not create json string");
        }
        logger.debug("Json String sent to server: " + json);
        return json;
    }

    private void writeJsonString(OutputStream outputStream, String jsonString) throws DifiStatisticsException {
        try {
            outputStream.write(jsonString.getBytes());
        }catch(IOException e){
            throw new DifiStatisticsException("Could not write json to output stream", e);
        }
    }
}

