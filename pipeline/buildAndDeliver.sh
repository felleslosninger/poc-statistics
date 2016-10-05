#!/usr/bin/env bash

version=$1

mvn clean versions:set -DnewVersion=${version} || exit 1
mvn deploy || exit 2
