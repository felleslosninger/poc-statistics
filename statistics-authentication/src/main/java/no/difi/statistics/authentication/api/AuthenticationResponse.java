package no.difi.statistics.authentication.api;

public class AuthenticationResponse {

    private boolean authenticated;

    private AuthenticationResponse() {
        // Use builder
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private AuthenticationResponse instance;

        Builder() {
            this.instance = new AuthenticationResponse();
        }

        public Builder authenticated(boolean authenticated) {
            instance.authenticated = authenticated;
            return this;
        }

        public AuthenticationResponse build() {
            try {
                return instance;
            } finally {
                instance = null;
            }
        }

    }

}
