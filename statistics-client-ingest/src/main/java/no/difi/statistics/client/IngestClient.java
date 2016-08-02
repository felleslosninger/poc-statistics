package no.difi.statistics.client;

import no.difi.statistics.client.exception.DifiStatisticsException;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.Map;

public class IngestClient {

    private static final String CONTENT_TYPE_KEY = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json";
//    TODO: Need working base URL for service
    private static final String SERVICE_BASE_URL = "http://localhost:8080";
    private static final String SERVICE_NAME = "minutes";

    private final JsonHelper jsonHelper;
    private final HttpHelper httpHelper;

    protected IngestClient(JsonHelper jsonHelper, HttpHelper httpHelper){
        this.jsonHelper = jsonHelper;
        this.httpHelper = httpHelper;
    }

    public IngestClient(){
        httpHelper = new HttpHelper();
        jsonHelper = new JsonHelper(httpHelper);

    }

    public void postMinutes(String seriesName, Map<String, Integer> measurements, ZonedDateTime timestamp) throws DifiStatisticsException {
        URL url = getUrl(seriesName);
        postMinutes(url, measurements, timestamp);
    }

    protected void postMinutes(URL url, Map<String, Integer> measurements, ZonedDateTime timestamp) throws DifiStatisticsException {

        HttpURLConnection conn = httpHelper.openConnection(url);
        conn.setDoOutput(true);
        httpHelper.setRequestMethod(conn);
        conn.setRequestProperty(CONTENT_TYPE_KEY, JSON_CONTENT_TYPE);
        OutputStream outputStream = jsonHelper.writeJsonToOutPutStream(measurements, timestamp, conn);
        httpHelper.flush(outputStream);
        httpHelper.controlResponse(conn);
        conn.disconnect();
    }

    private URL getUrl(String seriesName) throws DifiStatisticsException{
        URL url = null;
        try {
            url = new URL(SERVICE_BASE_URL + "/" + SERVICE_NAME + "/" + seriesName);
        } catch (MalformedURLException e) {
            throw new DifiStatisticsException("Could not get URL", e);
        }
        return url;
    }
}
