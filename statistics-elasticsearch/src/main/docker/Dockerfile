FROM elasticsearch:5.6.3
MAINTAINER Direktoratet for forvaltning og IKT <roy@difi.no>

HEALTHCHECK --timeout=10s --interval=10s --retries=60 \
    CMD curl -f 'http://localhost:9200'

COPY config ./config
