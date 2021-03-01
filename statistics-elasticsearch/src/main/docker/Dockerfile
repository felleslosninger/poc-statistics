FROM docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2
# Fetch oss==open source version of ElasticSearch

HEALTHCHECK --timeout=10s --interval=10s --retries=60 \
    CMD curl -f 'http://localhost:9200'

COPY config ./config
