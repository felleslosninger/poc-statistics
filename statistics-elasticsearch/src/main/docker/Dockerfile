FROM elasticsearch:2.4.1
MAINTAINER Direktoratet for forvaltning og IKT <roy@difi.no>

HEALTHCHECK --timeout=10s --interval=10s --retries=60 \
    CMD curl -f 'http://localhost:9200/_cluster/health?wait_for_status=green&timeout=5m'

COPY config ./config
