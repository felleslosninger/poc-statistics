#!/usr/bin/env bash

envscript=$(dirname $0)/../environment.sh
id='image-create'

vpcId=$(${envscript} createVpc ${id})
subnetId=$(${envscript} createSubnet ${id})
${envscript} createInternetGateway ${id}
${envscript} createRoute ${id}
${envscript} createSecurityGroup ${id}

packer build \
    -var "name=Statistics `date -u +"%Y%m%dT%H%M%SZ"`" \
    -var "subnet-id=${subnetId}" \
    -var "vpc-id=${vpcId}" \
    packer.json

${envscript} delete ${id}
