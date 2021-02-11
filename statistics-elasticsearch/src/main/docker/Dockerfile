FROM docker.elastic.co/elasticsearch/elasticsearch-oss:6.4.0

HEALTHCHECK --timeout=10s --interval=10s --retries=60 \
    CMD curl -f 'http://localhost:9200'

COPY config ./config
