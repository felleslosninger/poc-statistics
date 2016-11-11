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
    message=$1
    echo ${message}
    exit 1
}

warn() {
    message=$1
    echo ${message}
}

echoOk() {
    echo "[OK]"
}

requireArgument() {
    test -z ${!1} && fail "Missing argument '${1}'"
}

tag() {
    id=$1
    system_id=$2
    requireArgument 'id'
    requireArgument 'system_id'
    output=$(aws ec2 create-tags --resources ${id} --tags Key=SystemId,Value=${system_id}) || fail "Failed to tag resource"
}

filter() {
    version=$1
    requireArgument 'version'
    echo "Name=tag-key,Values=SystemId Name=tag-value,Values=$(systemId ${version})"
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
    echo -n "Creating VPC: "
    output=$(aws ec2 create-vpc --cidr-block $(cidrBlock)) || fail "Failed to create VPC"
    id=$(echo ${output} | jq -r ".Vpc.VpcId") || fail "Failed to get VPC id"
    echo -n "${id}"
    echo -n " Adding tag..."
    tag ${id} $(systemId ${version})
    echoOk
}

deleteVpc() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting VPC: "
    id=$(vpcId ${version})
    if [ ! -z ${id} ]; then
        echo -n "${id} "
        output=$(aws ec2 delete-vpc --vpc-id=${id}) || fail "Failed to delete VPC"
    else
        echo -n "Not found. "
    fi
    echoOk
}

createSubnet() {
    version=$1
    requireArgument 'version'
    echo -n "Creating subnet $(cidrBlock): "
    output=$(aws ec2 create-subnet --vpc-id $(vpcId ${version}) --cidr-block $(cidrBlock)) || fail "Failed to create subnet"
    id=$(echo ${output} | jq -r ".Subnet.SubnetId") || fail "Failed to get subnet id"
    echo -n "${id} "
    availability_zone=$(echo ${output} | jq -r ".Subnet.AvailabilityZone") || fail "Failed to get availability zone for subnet"
    echo -n "Using zone ${availability_zone} "
    echoOk
}

deleteSubnet() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting subnet: "
    output=$(aws ec2 describe-subnets --filters $(vpcFilter ${version})) || fail "Failed to find subnet"
    id=$(echo ${output} | jq -r ".Subnets[].SubnetId") || fail "Failed to get subnet id"
    if [ ! -z ${id} ]; then
        echo -n "${id} "
        aws ec2 delete-subnet --subnet-id=${id} || exit 4
    else
        echo -n "Not found. "
    fi
    echoOk
}

createInternetGateway() {
    version=$1
    requireArgument 'version'
    echo -n "Creating Internet gateway: "
    output=$(aws ec2 create-internet-gateway) || fail "Failed to create Internet gateway"
    id=$(echo ${output} | jq -r ".InternetGateway.InternetGatewayId") || fail "Failed to get Internet gateway id"
    echo -n "${id} "
    echo -n "Tagging... "
    tag ${id} $(systemId ${version})
    echo -n "Attaching... "
    output=$(aws ec2 attach-internet-gateway --internet-gateway-id ${id} --vpc-id $(vpcId ${version})) || fail "Failed to attach Internet gateway to VPC"
    echoOk
}

deleteInternetGateway() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting internet gateway: "
    output=$(aws ec2 describe-internet-gateways --filters $(filter ${version})) || fail "Failed to find Internet gateway"
    id=$(echo ${output} | jq -r ".InternetGateways[].InternetGatewayId") || fail "Failed to get id of Internet gateway"
    if [ ! -z ${id} ]; then
        echo -n "${id} "
        output=$(aws ec2 detach-internet-gateway --internet-gateway-id ${id} --vpc-id $(vpcId ${version})) || fail "Failed to detach Internet gateway"
        output=$(aws ec2 delete-internet-gateway --internet-gateway-id ${id}) || fail "Failed to delete Internet gateway"
    else
        echo -n "Not found. "
    fi
    echoOk
}

createRoute() {
    version=$1
    requireArgument 'version'
    echo -n "Creating route entry: "
    output=$(aws ec2 describe-route-tables --filters $(vpcFilter ${version})) || fail "Failed to find route table"
    rtb_id=$(echo ${output} | jq -r ".RouteTables[].RouteTableId") || fail "Failed to get route table id"
    echo -n "Found route table ${rtb_id}. "
    output=$(aws ec2 describe-internet-gateways --filters $(filter ${version})) || fail "Failed to find Internet gateway"
    igw_id=$(echo ${output} | jq -r ".InternetGateways[].InternetGatewayId") || fail "Failed to get Internet gateway id"
    echo -n "Found Internet gateway ${igw_id}. "
    output=$(aws ec2 create-route --route-table-id ${rtb_id} --destination-cidr-block 0.0.0.0/0 --gateway-id ${igw_id}) || fail "Failed to create route entry"
    echoOk
}

createSecurityGroup() {
    version=$1
    requireArgument 'version'
    echo -n "Creating security group: "
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
            "IpProtocol=TCP,FromPort=8080,ToPort=8082,IpRanges=[{CidrIp=0.0.0.0/0}]" \
            ) || fail "Failed to add ingress rules to security group"
    echo -n "Adding tag... "
    tag ${id} $(systemId ${version})
    echoOk
}

deleteSecurityGroup() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting security group: "
    output=$(aws ec2 describe-security-groups --filters $(filter ${version})) || fail "Failed to find security group"
    id=$(echo ${output} | jq -r ".SecurityGroups[].GroupId") || fail "Failed to get id of security group"
    if [ ! -z ${id} ]; then
        echo -n "${id} "
        output=$(aws ec2 delete-security-group --group-id ${id}) || fail "Failed to delete security group"
    else
        echo -n "Not found. "
    fi
    echoOk
}

deleteRunningInstances() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting running EC2 instances: "
    output=$(aws ec2 describe-instances --filter $(filter ${version}) Name=instance-state-name,Values=running) || fail "Failed to find running EC2 instances"
    ids=$(echo ${output} | jq -r ".Reservations[].Instances[].InstanceId" | paste -s -d ' ' -) || fail "Failed to get ids of running EC2 instances"
    if [ ! -z "${ids}" ]; then
        echo -n "(${ids}) "
        output=$(aws ec2 terminate-instances --instance-ids ${ids}) || fail "Failed to terminate running EC2 instances"
    else
        echo -n "None found. "
    fi
    echoOk
}

waitForInstancesToTerminate() {
    version=$1
    requireArgument 'version'
    echo -n "Waiting for EC2 instances to terminate... "
    ids=all
    while [[ "${ids}" != "" ]]; do
        output=$(aws ec2 describe-instances --filter $(filter ${version}) Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped) || fail "Failed to find EC2 instances"
        ids=$(echo ${output} | jq -r ".Reservations[].Instances[].InstanceId" | paste -s -d ' ' -) || fail "Failed to find ids of EC2 instances"
        if [ ! -z "${ids}" ]; then
            echo -n "(${ids}) "
            sleep 3
        fi
    done
    echoOk
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
        --amazonec2-instance-type c4.large"
}

virtualBoxDockerParams() {
    version=$1
    requireArgument 'version'
    echo -n "\
        --engine-label system_id=$(systemId ${version}) \
        -d virtualbox"
}

createDockerMachines() {
    version=$1
    requireArgument 'version'
    driver=${2-'amazonec2'}
    echo "Creating Docker machines..."
    case ${driver} in
        'amazonec2')
            params=$(awsDockerParams ${version})
            ;;
        'virtualbox')
            params=$(virtualBoxDockerParams ${version})
            ;;
        "*")
            fail "Unsupported docker-machine driver ${driver}"
            ;;
    esac
    docker-machine create ${params} $(nodeName ${version} '01') &
    child_pids="${child_pids} $!"
    docker-machine create ${params} $(nodeName ${version} '02') &
    child_pids="${child_pids} $!"
    docker-machine create ${params} $(nodeName ${version} '03') &
    child_pids="${child_pids} $!"
    wait
    [ "${driver}" == 'virtualbox' ] && \
        { docker-machine ssh $(nodeName ${version} '01') tce-load -wi bash || fail; }
    echoOk
}

deleteDockerMachines() {
    version=$1
    requireArgument 'version'
    echo -n "Deleting Docker machines: "
    machines=$(docker-machine ls --filter "label=system_id=$(systemId ${version})" --format "{{.Name}}" | paste -s -d ' ' -)
    if [ ! -z "${machines}" ]; then
        echo -n "(${machines}) "
        output=$(docker-machine rm -f ${machines}) || exit 1
    else
        echo -n "None found. "
    fi
    echoOk
}

joinDockerSwarm() {
    node_name=$1
    swarm_token=$2
    swarm_address=$3
    requireArgument 'node_name'
    requireArgument 'swarm_token'
    requireArgument 'swarm_address'
    echo -n "Node ${node_name} is joining Docker swarm... "
    output=$(docker-machine ssh ${node_name} sudo docker swarm join --token ${swarm_token} ${swarm_address}) || fail "Failed to join ${node_name} to Docker swarm"
    echoOk
}

setupDockerSwarm() {
    version=$1
    requireArgument 'version'
    driver=${2-'amazonec2'}
    managerNode=$(nodeName ${version} '01')
    echo "Creating Docker swarm..."
    advertiseAddress='eth0'
    [ "${driver}" == 'virtualbox' ] && advertiseAddress='eth1'
    output=$(docker-machine ssh ${managerNode} sudo docker swarm init --advertise-addr ${advertiseAddress}) || fail "Failed to initialize Docker swarm"
    swarm_address=$(docker-machine ssh ${managerNode} sudo docker node inspect self | jq -r ".[].ManagerStatus.Addr") || fail "Failed to get address of Docker swarm manager"
    swarm_token=$(docker-machine ssh ${managerNode} sudo docker swarm join-token -q worker) || fail "Failed to get Docker swarm's join token"
    joinDockerSwarm $(nodeName ${version} '02') ${swarm_token} ${swarm_address}
    joinDockerSwarm $(nodeName ${version} '03') ${swarm_token} ${swarm_address}
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
    [ "${driver}" == 'amazonec2' ] && {
        deleteRunningInstances ${version}
        waitForInstancesToTerminate ${version}
        deleteSubnet ${version}
        deleteSecurityGroup ${version}
        deleteInternetGateway ${version}
        deleteVpc ${version}
    }
    deleteDockerMachines ${version}
}

case $1 in *)
        function=$1
        shift
        ${function} "$@"
        ;;
esac
