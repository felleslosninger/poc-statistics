#!/usr/bin/env bash

set -e

pid=0

# Graceful shutdown on SIGTERM. Try to wait for green status of cluster.
# Note that Docker's grace period defaults to 10 seconds, so this should also be
# increased for this to have effect.
shutdownGracefully() {
  curl -XGET 'http://localhost:9200/_cluster/health?wait_for_status=green&timeout=5m'
  if [ $pid -ne 0 ]; then
    kill $pid
    wait $pid
  fi
  exit 143; # 128 + 15 -- SIGTERM
}
trap shutdownGracefully SIGTERM

# Add elasticsearch as command if needed
if [ "${1:0:1}" = '-' ]; then
	set -- elasticsearch "$@"
fi

# Drop root privileges if we are running elasticsearch
# allow the container to be started with `--user`
if [ "$1" = 'elasticsearch' -a "$(id -u)" = '0' ]; then
	# Change the ownership of /usr/share/elasticsearch/data to elasticsearch
	chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/data

	set -- gosu elasticsearch "$@"
fi

"$@" &
pid=$!
wait
