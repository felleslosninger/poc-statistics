#!/usr/bin/env bash

export DOCKER_REGISTRY=local
export DOCKER_IMAGE_VERSION=$(docker image ls --format "{{.Tag}}" local/statistics-elasticsearch |head -1)
echo "Reusing newest docker image version: "${DOCKER_IMAGE_VERSION}

docker stack rm statistics

export VOLUME_EXISTS=$(docker volume ls | grep statistics-data | wc -l)

if [ $VOLUME_EXISTS -eq 1 ]
then
echo "Volume statistics-data exists, wait for dependent services/containers to be deleted before delete of volume succeeds..."
until docker volume rm statistics-data &> /dev/null; do sleep 1 ; done # Tries once a second to delete
fi

VERSION=${DOCKER_IMAGE_VERSION} REGISTRY=${DOCKER_REGISTRY} \
    docker stack deploy -c docker/stack.yml --prune --resolve-image=never statistics || exit 1

echo "Deployed services using locally built image(s) with tag ${DOCKER_IMAGE_VERSION}"
