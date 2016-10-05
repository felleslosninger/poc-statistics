#!/usr/bin/env bash

masterNode=$1

ssh ${masterNode} \
    curl -sS -XPUT ${masterNode}:9200/_cluster/settings -d '\{\"transient\":\{\"cluster.routing.allocation.enable\":\"none\"\}\}'
