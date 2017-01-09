#!/usr/bin/env bash

finish() {
    echo "Child processes to kill: ${child_pids}"
    for child_pid in ${child_pids}; do
        if kill -0 ${child_pid}; then
            echo -n "Sending termination signal to child pid ${child_pid}..."
            kill -TERM ${child_pid}
            echo " [$?]"
        fi
    done
}
trap finish TERM INT

fail() {
    local ret=$?
    local message=${1-"Failed"}
    ret=${2-${ret}}
    echo "[${message} (${ret})]"
    return ${ret}
}

die() {
    local ret=$?
    local message=${1-"Failed"}
    ret=${2-${ret}}
    echo "[${message} (${ret})]"
    exit ${ret}
}

warn() {
    local message=$1
    echo ${message}
}

ok() {
    local start=$1
    echo "[OK${start:+ ($(duration ${start})s)}]"
}

requireArgument() {
    test -z "${!1}" && die "Missing argument '${1}'" 1
}

duration() {
    local from=$1
    requireArgument 'from'
    echo -n $(( SECONDS - ${from} ))
}

waitFor() {
    local fun=$1
    requireArgument 'fun'
    local duration=${2-100}
    local status=false
    for i in $(seq 1 ${duration}); do
        local ret
        ${fun}
        ret=$?
        [ ${ret} -eq 7 ] && { >&2 echo -n "."; sleep 3; } # Connect failure
        [ ${ret} -eq 28 ] && { >&2 echo -n "_"; sleep 3; } # Request timeout
        [ ${ret} -eq 0 ] && { status=true; break; }
        [ ${ret} -eq 1 ] && break
    done
    ${status} && return 0 || return 1
}

tag() {
    id=$1
    system_id=$2
    requireArgument 'id'
    requireArgument 'system_id'
    output=$(aws ec2 create-tags --resources ${id} --tags Key=SystemId,Value=${system_id}) || fail "Failed to tag resource"
    output=$(aws ec2 create-tags --resources ${id} --tags Key=Name,Value=${system_id}) || fail "Failed to set name on VPC"
}

nameTag() {
    nodeName=$1
    requireArgument 'nodeName'
    echo "Key=Name,Value=${nodeName}"
}

systemIdTag() {
    version=$1
    requireArgument 'version'
    echo "Key=SystemId,Value=$(systemId ${version})"
}

filter() {
    version=$1
    requireArgument 'version'
    echo "Name=tag-key,Values=SystemId Name=tag-value,Values=$(systemId ${version})"
}

nameFilter() {
    name=$1
    requireArgument 'name'
    echo "Name=tag-key,Values=Name Name=tag-value,Values=${name}"
}

runningFilter() {
    echo "Name=instance-state-name,Values=running"
}

systemId() {
    version=$1
    requireArgument 'version'
    echo "statistics-${version}"
}

nodeName() {
    version=$1
    node_number=$2
    requireArgument 'version'
    requireArgument 'node_number'
    echo "$(systemId ${version})-node${node_number}"
}

vpcId() {
    version=$1
    requireArgument 'version'
    output=$(aws ec2 describe-vpcs --filters $(filter ${version})) || fail "Failed to find VPC"
    id=$(echo ${output} | jq -r ".Vpcs[].VpcId") || fail "Failed to get VPC id"
    echo "${id}"
}

vpcFilter() {
    version=$1
    requireArgument 'version'
    echo "Name=vpc-id,Values=$(vpcId ${version})"
}

cidrBlock() {
    echo "172.35.0.0/16"
}

createVpc() {
    version=$1
    requireArgument 'version'
    output=$(aws ec2 create-vpc --cidr-block $(cidrBlock)) || fail "Failed to create VPC"
    id=$(echo ${output} | jq -r ".Vpc.VpcId") || fail "Failed to get VPC id"
    output=$(aws ec2 modify-vpc-attribute --vpc-id ${id} --enable-dns-hostnames "{\"Value\":true}") || fail "Failed to enable DNS hostnames on VPC"
    tag ${id} $(systemId ${version})
    echo -n "${id}"
}

deleteVpc() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting VPC: "
    local start=${SECONDS}
    id=$(vpcId ${version})
    if [ ! -z ${id} ]; then
        echo -n "${id} "
        output=$(aws ec2 delete-vpc --vpc-id=${id}) || fail "Failed to delete VPC"
    else
        echo -n "Not found. "
    fi
    ok ${start}
}

createSubnet() {
    version=$1
    requireArgument 'version'
    output=$(aws ec2 create-subnet --vpc-id $(vpcId ${version}) --cidr-block $(cidrBlock)) || fail "Failed to create subnet"
    id=$(echo ${output} | jq -r ".Subnet.SubnetId") || fail "Failed to get subnet id"
    echo -n "${id} "
}

deleteSubnet() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting subnet: "
    local start=${SECONDS}
    output=$(aws ec2 describe-subnets --filters $(vpcFilter ${version})) || fail "Failed to find subnet"
    id=$(echo ${output} | jq -r ".Subnets[].SubnetId") || fail "Failed to get subnet id"
    if [ ! -z ${id} ]; then
        echo -n "${id} "
        aws ec2 delete-subnet --subnet-id=${id} || exit 4
    else
        echo -n "Not found. "
    fi
    ok ${start}
}

createInternetGateway() {
    version=$1
    requireArgument 'version'
    echo -n "Creating Internet gateway: "
    local start=${SECONDS}
    output=$(aws ec2 create-internet-gateway) || fail "Failed to create Internet gateway"
    id=$(echo ${output} | jq -r ".InternetGateway.InternetGatewayId") || fail "Failed to get Internet gateway id"
    echo -n "${id} "
    echo -n "Tagging... "
    tag ${id} $(systemId ${version})
    echo -n "Attaching... "
    output=$(aws ec2 attach-internet-gateway --internet-gateway-id ${id} --vpc-id $(vpcId ${version})) || fail "Failed to attach Internet gateway to VPC"
    ok ${start}
}

deleteInternetGateway() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting internet gateway: "
    local start=${SECONDS}
    output=$(aws ec2 describe-internet-gateways --filters $(filter ${version})) || fail "Failed to find Internet gateway"
    id=$(echo ${output} | jq -r ".InternetGateways[].InternetGatewayId") || fail "Failed to get id of Internet gateway"
    if [ ! -z ${id} ]; then
        echo -n "${id} "
        output=$(aws ec2 detach-internet-gateway --internet-gateway-id ${id} --vpc-id $(vpcId ${version})) || fail "Failed to detach Internet gateway"
        output=$(aws ec2 delete-internet-gateway --internet-gateway-id ${id}) || fail "Failed to delete Internet gateway"
    else
        echo -n "Not found. "
    fi
    ok ${start}
}

createRoute() {
    version=$1
    requireArgument 'version'
    echo -n "Creating route entry: "
    local start=${SECONDS}
    output=$(aws ec2 describe-route-tables --filters $(vpcFilter ${version})) || fail "Failed to find route table"
    rtb_id=$(echo ${output} | jq -r ".RouteTables[].RouteTableId") || fail "Failed to get route table id"
    echo -n "Found route table ${rtb_id}. "
    output=$(aws ec2 describe-internet-gateways --filters $(filter ${version})) || fail "Failed to find Internet gateway"
    igw_id=$(echo ${output} | jq -r ".InternetGateways[].InternetGatewayId") || fail "Failed to get Internet gateway id"
    echo -n "Found Internet gateway ${igw_id}. "
    output=$(aws ec2 create-route --route-table-id ${rtb_id} --destination-cidr-block 0.0.0.0/0 --gateway-id ${igw_id}) || fail "Failed to create route entry"
    ok ${start}
}

createSecurityGroup() {
    version=$1
    requireArgument 'version'
    echo -n "Creating security group: "
    local start=${SECONDS}
    sg_name="$(systemId ${version})"
    output=$(aws ec2 create-security-group --vpc-id $(vpcId ${version}) --group-name ${sg_name} --description "Security group for system id $(systemId ${version})") || fail "Failed to create security group"
    id=$(echo ${output} | jq -r ".GroupId") || fail "Failed to get security group id"
    echo -n "${id}"
    echo -n "Adding rules... "
    output=$(aws ec2 authorize-security-group-ingress --group-id ${id} \
        --ip-permissions \
            "IpProtocol=TCP,FromPort=22,ToPort=22,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=TCP,FromPort=2376,ToPort=2376,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=TCP,FromPort=2377,ToPort=2377,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=TCP,FromPort=4789,ToPort=4789,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=UDP,FromPort=4789,ToPort=4789,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=TCP,FromPort=7946,ToPort=7946,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=UDP,FromPort=7946,ToPort=7946,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=TCP,FromPort=9200,ToPort=9201,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=TCP,FromPort=9300,ToPort=9301,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            "IpProtocol=TCP,FromPort=8080,ToPort=8083,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            ) || fail "Failed to add ingress rules to security group"
    echo -n "Adding tag... "
    tag ${id} $(systemId ${version})
    ok ${start}
}

deleteSecurityGroup() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting security group: "
    local start=${SECONDS}
    output=$(aws ec2 describe-security-groups --filters $(filter ${version})) || fail "Failed to find security group"
    id=$(echo ${output} | jq -r ".SecurityGroups[].GroupId") || fail "Failed to get id of security group"
    if [ ! -z ${id} ]; then
        echo -n "${id} "
        output=$(aws ec2 delete-security-group --group-id ${id}) || fail "Failed to delete security group"
    else
        echo -n "Not found. "
    fi
    ok ${start}
}

deleteRunningInstances() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting running EC2 instances: "
    local start=${SECONDS}
    output=$(aws ec2 describe-instances --filter $(filter ${version}) $(runningFilter)) || fail "Failed to find running EC2 instances"
    ids=$(echo ${output} | jq -r ".Reservations[].Instances[].InstanceId" | paste -s -d ' ' -) || fail "Failed to get ids of running EC2 instances"
    if [ ! -z "${ids}" ]; then
        echo -n "(${ids}) "
        output=$(aws ec2 terminate-instances --instance-ids ${ids}) || fail "Failed to terminate running EC2 instances"
    else
        echo -n "None found. "
    fi
    ok ${start}
}

isInstanceTerminatedOrNonExisting() {
    local instanceId=$1
    requireArgument 'instanceId'
    local output
    output=$(2>&1 aws ec2 describe-instances --instance-ids ${instanceId})
    local rc=$?
    [ ${rc} -eq 255 ] && echo ${output} | grep -q "InvalidInstanceID.NotFound" && return 0
    [ ! ${rc} -eq 0 ] && return 1
    [ "$(echo ${output} | jq -r ".Reservations[].Instances[].State.Name")" == "terminated" ] && return 0
    [ "$(echo ${output} | jq -r ".Reservations[].Instances[].State.Name")" == "running" ] && return 28
    [ "$(echo ${output} | jq -r ".Reservations[].Instances[].State.Name")" == "shutting-down" ] && return 28
    [ "$(echo ${output} | jq -r ".Reservations[].Instances[].State.Name")" == "stopping" ] && return 28
    return 1
}

terminateNode() {
    local nodeName=$1
    requireArgument 'nodeName'
    source .environment/${nodeName} 2>/dev/null || die "Unknown node ${nodeName}"
    local output
    output=$(2>&1 aws ec2 terminate-instances --instance-ids ${aws_instance_id})
    local rc=$?
    [ ${rc} -eq 255 ] && echo ${output} | grep -q "InvalidInstanceID.NotFound" && return 0
    waitFor "isInstanceTerminatedOrNonExisting ${aws_instance_id}"
}

waitForInstancesToTerminate() {
    version=$1
    requireArgument 'version'
    echo -n "Waiting for EC2 instances to terminate... "
    local start=${SECONDS}
    ids=all
    while [[ "${ids}" != "" ]]; do
        output=$(aws ec2 describe-instances --filter $(filter ${version}) Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped) || fail "Failed to find EC2 instances"
        ids=$(echo ${output} | jq -r ".Reservations[].Instances[].InstanceId" | paste -s -d ' ' -) || fail "Failed to find ids of EC2 instances"
        if [ ! -z "${ids}" ]; then
            echo -n "(${ids}) "
            sleep 3
        fi
    done
    ok ${start}
}

awsDockerParams() {
    version=$1
    requireArgument 'version'
    system_id=$(systemId ${version})
    vpc_id=$(vpcId ${version})
    output=$(aws ec2 describe-subnets --filters $(vpcFilter ${version})) || fail "Failed to find subnet"
    subnet_id=$(echo ${output} | jq -r ".Subnets[].SubnetId") || fail "Failed to get subnet id"
    availability_zone=$(echo ${output} | jq -r ".Subnets[].AvailabilityZone") || fail "Failed to get availability zone for subnet"
    output=$(aws ec2 describe-security-groups --filters $(filter ${version})) || fail "Failed to find security group"
    sg_name=$(echo ${output} | jq -r ".SecurityGroups[].GroupName") || fail "Failed to get security group name"
    echo -n "\
        --engine-label system_id=${system_id} \
        -d amazonec2 \
        --amazonec2-tags SystemId,${system_id} \
        --amazonec2-vpc-id ${vpc_id} \
        --amazonec2-subnet-id ${subnet_id} \
        --amazonec2-zone ${availability_zone:${#availability_zone}-1:1} \
        --amazonec2-security-group ${sg_name} \
        --amazonec2-instance-type c4.large \
        "
#        --amazonec2-ssh-user rancher \
#        --amazonec2-ami ami-dfdff3c8"
}

virtualBoxDockerParams() {
    version=$1
    requireArgument 'version'
    echo -n "\
        --engine-label system_id=$(systemId ${version}) \
        --virtualbox-memory 8192 \
        -d virtualbox"
}

keyFile() {
    keyName=$1
    requireArgument 'keyName'
    echo "$(pwd)/.environment/key_${keyName}"
}

createKeyPairOnAws() {
    version=$1
    requireArgument 'version'
    mkdir -p .environment
    keyName=$(systemId ${version})
    keyFile=$(keyFile ${keyName})
    aws ec2 create-key-pair --key-name ${keyName} --query 'KeyMaterial' --output text > ${keyFile} || fail "Failed to create key ${keyName} on AWS"
    chmod 0400 ${keyFile}
    echo ${keyName}
}

deleteKeyPairOnAws() {
    version=$1
    requireArgument 'version'
    keyName=$(systemId ${version})
    aws ec2 delete-key-pair --key-name ${keyName} || fail "Failed to delete key ${keyName} from AWS"
    keyFile=$(keyFile ${keyName})
    rm -f ${keyFile} || fail "Failed to delete key file"
}

findPublicIp() {
    nodeName=$1
    requireArgument 'nodeName'
    local i
    for i in {1..100}; do
        output=$(aws ec2 describe-instances --filter $(nameFilter ${nodeName}) $(runningFilter)) || fail "Failed to describe ${node_name}"
        public_ip=$(echo ${output} | jq -r ".Reservations[].Instances[].PublicIpAddress") || fail "Failed to get IP of node ${node_name}"
        [[ ! -z ${public_ip} ]] && break;
        sleep 3
    done
    echo ${public_ip}
}

createDockerMachinesOnAws() {
    version=$1
    requireArgument 'version'
    count=${2-3}
    output=$(aws ec2 describe-subnets --filters $(vpcFilter ${version})) || fail "Failed to find subnet"
    subnet_id=$(echo ${output} | jq -r ".Subnets[].SubnetId") || fail "Failed to get subnet id"
    output=$(aws ec2 describe-security-groups --filters $(filter ${version})) || fail "Failed to find security group"
    sg_id=$(echo ${output} | jq -r ".SecurityGroups[].GroupId") || fail "Failed to get security group id"
    keyName=$(createKeyPairOnAws ${version})
    output=$(aws ec2 run-instances \
	    --image-id ami-ec3421fb \
	    --security-group-ids ${sg_id} \
	    --instance-type c4.xlarge \
	    --subnet-id ${subnet_id} \
	    --count ${count} \
	    --associate-public-ip-address \
	    --user-data "#!/bin/bash
mkdir -p /usr/share/elasticsearch/data" \
	    --key-name ${keyName})
    local i
	for (( i=0; i<${count}; i++ )); do
        node_names[$i]=$(nodeName ${version} $((i + 1)))
	    instance_ids[$i]=$(echo ${output} | jq -r ".Instances[${i}].InstanceId" | sed 's/"//g')
        echo "aws_instance_id=${instance_ids[$i]}" >> .environment/${node_names[$i]}
        aws ec2 create-tags --resources ${instance_ids[$i]} --tags $(nameTag ${node_names[$i]}) $(systemIdTag ${version}) &
    done
    for (( i=0; i<${count}; i++ )); do
        node_name=${node_names[$i]}
        publicIp=$(findPublicIp ${node_name})
        echo "Node ${node_name} has address ${publicIp}"
        echo "address=${publicIp}" >> .environment/${node_name}
        echo "ssh_key_file=$(keyFile ${keyName})" >> .environment/${node_name}
    done
}

createDockerMachines() {
    version=$1
    requireArgument 'version'
    driver=${2-'amazonec2'}
    echo "Creating Docker machines..."
    local start=${SECONDS}
    case ${driver} in
        'amazonec2')
            createDockerMachinesOnAws ${version} || fail "Failed to create Docker Machines"
            ;;
        'amazonec2-legacy')
            params=$(awsDockerParams ${version})
            docker-machine create ${params} $(nodeName ${version} '1') &
            child_pids="${child_pids} $!"
            docker-machine create ${params} $(nodeName ${version} '2') &
            child_pids="${child_pids} $!"
            docker-machine create ${params} $(nodeName ${version} '3') &
            child_pids="${child_pids} $!"
            wait
            ;;
        'virtualbox')
            params=$(virtualBoxDockerParams ${version})
            docker-machine create ${params} $(nodeName ${version} '1') &
            child_pids="${child_pids} $!"
            docker-machine create ${params} $(nodeName ${version} '2') &
            child_pids="${child_pids} $!"
            docker-machine create ${params} $(nodeName ${version} '3') &
            child_pids="${child_pids} $!"
            wait
            local postCommand="sudo -- sh -c 'sysctl -w vm.max_map_count=262144; mkdir -p /usr/share/elasticsearch/data'"
            login $(nodeName ${version} '1') tce-load -wi bash || fail "Failed to install bash on node 1"
            login $(nodeName ${version} '1') "${postCommand}" || fail "Failed to apply post-configuration to node 1"
            login $(nodeName ${version} '2') "${postCommand}" || fail "Failed to apply post-configuration to node 2"
            login $(nodeName ${version} '3') "${postCommand}" || fail "Failed to apply post-configuration to node 3"
            ;;
        "*")
            fail "Unsupported docker-machine driver ${driver}"
            ;;
    esac
    ok ${start}
}

deleteDockerMachines() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting Docker machines: "
    local start=${SECONDS}
    machines=$(docker-machine ls --filter "label=system_id=$(systemId ${version})" --format "{{.Name}}" | paste -s -d ' ' -)
    if [ ! -z "${machines}" ]; then
        echo -n "(${machines}) "
        output=$(docker-machine rm -f ${machines}) || exit 1
    else
        echo -n "None found. "
    fi
    ok ${start}
}

joinDockerSwarm() {
    node_name=$1
    swarm_token=$2
    swarm_address=$3
    requireArgument 'node_name'
    requireArgument 'swarm_token'
    requireArgument 'swarm_address'
    echo -n "Node ${node_name} is joining Docker swarm... "
    local start=${SECONDS}
    output=$(login ${node_name} sudo docker swarm join --token ${swarm_token} ${swarm_address}) || fail "Failed to join ${node_name} to Docker swarm"
    ok ${start}
}

setupDockerSwarm() {
    version=$1
    requireArgument 'version'
    driver=${2-'amazonec2'}
    managerNode=$(nodeName ${version} '1')
    echo "Creating Docker swarm..."
    advertiseAddress='ens3'
    [ "${driver}" == 'virtualbox' ] && advertiseAddress='eth1'
    output=$(login ${managerNode} sudo docker swarm init --advertise-addr ${advertiseAddress}) || fail "Failed to initialize Docker swarm: ${output}"
    swarm_address=$(login ${managerNode} sudo docker node inspect self | jq -r ".[].ManagerStatus.Addr") || fail "Failed to get address of Docker swarm manager"
    swarm_token=$(login ${managerNode} sudo docker swarm join-token -q manager) || fail "Failed to get Docker swarm's join token"
    joinDockerSwarm $(nodeName ${version} '2') ${swarm_token} ${swarm_address}
    joinDockerSwarm $(nodeName ${version} '3') ${swarm_token} ${swarm_address}
    echo "Docker swarm created successfully"
}

create() {
    version=$1
    requireArgument 'version'
    driver=${2-'amazonec2'}
    [ "${driver}" == 'amazonec2' ] && {
        createVpc ${version}
        createSubnet ${version}
        createInternetGateway ${version}
        createRoute ${version}
        createSecurityGroup ${version}
    }
    createDockerMachines ${version} ${driver}
    setupDockerSwarm ${version} ${driver}
}

delete() {
    version=$1
    driver=${2-'amazonec2'}
    requireArgument 'version'
    [ "${driver}" == 'amazonec2' -o "${driver}" == 'amazonec2-legacy' ] && {
        deleteRunningInstances ${version}
        deleteKeyPairOnAws ${version}
        waitForInstancesToTerminate ${version}
        deleteSubnet ${version}
        deleteSecurityGroup ${version}
        deleteInternetGateway ${version}
        deleteVpc ${version}
    }
    deleteDockerMachines ${version}
    find .environment -regex ".environment/statistics-${version}-node\d\+" -exec rm {} \;
}

login() {
    local nodeName=$1
    requireArgument 'nodeName'
    shift
    local sshCommand
    docker-machine inspect ${nodeName} > /dev/null && {
        sshCommand='docker-machine ssh ${nodeName} "$@"'
    } || {
        source .environment/${nodeName} 2>/dev/null || fail "Unknown node ${nodeName}"
        sshCommand="ssh -o 'StrictHostKeyChecking no' -i ${ssh_key_file} ubuntu@${address} $@"
    }
    local i
    for i in {1..10}; do
        eval ${sshCommand}
        ret=$?
        [ ${ret} -eq 0 ] && break;
        echo "Command failed: [${sshCommand}] (${ret})"
        [ ! ${ret} -eq 255 ] && return ${ret};
        sleep 3
    done
}

case $1 in *)
        function=$1
        shift
        ${function} "$@"
        ;;
esac
