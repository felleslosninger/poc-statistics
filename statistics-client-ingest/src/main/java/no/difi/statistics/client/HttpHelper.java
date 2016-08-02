package no.difi.statistics.client;

import no.difi.statistics.client.exception.DifiStatisticsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;

public class HttpHelper {

    private static final String REQUEST_METHOD_POST = "POST";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected OutputStream getOutputStream(HttpURLConnection conn) throws DifiStatisticsException {
        OutputStream outputStream = null;
        try {
            outputStream = conn.getOutputStream();
        }catch(java.io.IOException e){
            throw new DifiStatisticsException("Could not get Output stream", e);
        }
        if(outputStream==null){
            throw new DifiStatisticsException("Could not get Output stream");
        }
        return outputStream;
    }

    protected void controlResponse(HttpURLConnection conn) throws DifiStatisticsException {
        int responseCode = 0;
        try {
            responseCode = conn.getResponseCode();
        }catch(IOException e) {
            throw new DifiStatisticsException("Could not get response code", e);
        }
        if(responseCode != HttpURLConnection.HTTP_OK){
            throw new DifiStatisticsException("Response code was" + responseCode);
        }
        logger.debug("Response was " + responseCode);
    }

    protected void flush(OutputStream outputStream) throws DifiStatisticsException {
        try {
            outputStream.flush();
        }catch(IOException e){
            throw new DifiStatisticsException("Could not flush output stream", e);
        }
    }

    protected void setRequestMethod(HttpURLConnection conn) throws DifiStatisticsException {
        try {
            conn.setRequestMethod(REQUEST_METHOD_POST);
        } catch (ProtocolException e) {
            throw new DifiStatisticsException("Could not set request method", e);
        }
    }

    protected HttpURLConnection openConnection(URL url) throws DifiStatisticsException{
        HttpURLConnection conn = null;
        try {
            conn = (HttpURLConnection) url.openConnection();
        } catch (IOException e) {
            throw new DifiStatisticsException("Could not open connection", e);
        }
        if(conn==null){
            throw new DifiStatisticsException("Connection is not set correctly");
        }
        return conn;
    }
}
