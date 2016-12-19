#!/usr/bin/env bash

fail() {
    message=$1
    echo ${message}
    exit 1
}

requireArgument() {
    test -z ${!1} && fail "Missing argument '${1}'"
}

subnetId=$1
vpcId=$2
requireArgument 'subnetId'
requireArgument 'vpcId'

packer build \
    -var "name=Statistics `date -u +"%Y%m%dT%H%M%SZ"`" \
    -var "subnet-id=${subnetId}" \
    -var "vpc-id=${vpcId}" \
    packer.json