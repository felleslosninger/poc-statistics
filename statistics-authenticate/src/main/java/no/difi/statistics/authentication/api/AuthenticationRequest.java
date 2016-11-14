package no.difi.statistics.authentication.api;

public class AuthenticationRequest {

    private String username;
    private String password;

    public AuthenticationRequest() {
        // Use builder
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static AuthenticationRequest.Builder builder() {
        return new AuthenticationRequest.Builder();
    }

    public static class Builder {
        private AuthenticationRequest instance = new AuthenticationRequest();

        public Builder username(String username) {
            instance.username = username;
            return this;
        }

        public Builder password(String password) {
            instance.password = password;
            return this;
        }

        public AuthenticationRequest build() {
            try {
                return instance;
            } finally {
                instance = null;
            }
        }

    }

    

}
