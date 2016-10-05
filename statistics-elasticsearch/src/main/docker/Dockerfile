FROM elasticsearch:2.4.1
MAINTAINER Direktoratet for forvaltning og IKT <roy@difi.no>

COPY config ./config
COPY docker-graceful-entrypoint.sh /bin/

ENTRYPOINT ["/bin/docker-graceful-entrypoint.sh"]
