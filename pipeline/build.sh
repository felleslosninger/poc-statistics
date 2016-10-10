#!/usr/bin/env bash

verify() {
    mvn clean verify || exit 1
}

deliver() {
    version=$1
    mvn clean versions:set -DnewVersion=${version} || exit 1
    mvn deploy || exit 2
}

case $1 in
    verify)
        verify
        ;;
    deliver)
        deliver $2
        ;;
esac
