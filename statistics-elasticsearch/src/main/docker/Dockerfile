FROM elasticsearch:5.6.3
MAINTAINER Direktoratet for forvaltning og IKT <roy@difi.no>

HEALTHCHECK --timeout=10s --interval=10s --retries=60 \
    CMD curl -f 'http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=10s'

COPY config ./config
