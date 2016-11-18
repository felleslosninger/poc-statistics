package no.difi.statistics.test.utils;

import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import static org.apache.commons.codec.binary.Base64.encodeBase64;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import org.springframework.http.MediaType;

public class MockMvcRequests {

    public static RequestBuilder request() {
        return new RequestBuilder();
    }

    public static class RequestBuilder {
        private String owner = "aUser";
        private String series = "aTimeSeries";
        private String user = "aUser";
        private String password = "aPassword";
        private String content;
        private String distance;

        public RequestBuilder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public RequestBuilder series(String series) {
            this.series = series;
            return this;
        }

        public RequestBuilder user(String user) {
            this.user = user;
            return this;
        }

        public RequestBuilder password(String password) {
            this.password = password;
            return this;
        }

        public RequestBuilder content(String content) {
            this.content = content;
            return this;
        }

        public RequestBuilder distance(String distance) {
            this.distance = distance;
            return this;
        }

        private String authorizationHeader(String username, String password) {
            return "Basic " + new String(encodeBase64((username + ":" + password).getBytes()));
        }

        public MockHttpServletRequestBuilder ingest() {
            return post("/{owner}/{seriesName}/{distance}", owner, series, distance)
                    .contentType(MediaType.APPLICATION_JSON_UTF8)
                    .header("Authorization", authorizationHeader(user, password))
                    .content(content);
        }

        public MockHttpServletRequestBuilder last() {
            return get("/{owner}/{seriesName}/{distance}/last", owner, series, distance);
        }

    }

}
