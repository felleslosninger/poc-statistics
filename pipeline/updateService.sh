#!/usr/bin/env bash

service=$1
image=$2
version=$3
masterNode=$4
delay=$5

ssh ${masterNode} \
    docker service update --update-delay ${delay}s --image ${image}:${version} ${service} \
    && echo ${service} updated to version ${version}