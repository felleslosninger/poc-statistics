#!/usr/bin/env bash

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

dotSleep() {
    local length=${1-1}
    echo -n "."
    sleep ${length};
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
        [ ${ret} -eq 7 ] && { >&2 echo -n "."; sleep 1; } # Connect failure
        [ ${ret} -eq 28 ] && { >&2 echo -n "_"; sleep 1; } # Request timeout
        [ ${ret} -eq 0 ] && { status=true; break; }
        [ ${ret} -eq 1 ] && break
    done
    ${status} && return 0 || return 1
}

image() {
    local service=$1
    local version=${2-'latest'}
    requireArgument 'service'
    case "${service}" in
        "elasticsearch")
            image="difi/statistics-elasticsearch:${version}"
            ;;
        "elasticsearch_gossip")
            image="difi/statistics-elasticsearch:${version}"
            ;;
        "query")
            image="difi/statistics-query-elasticsearch:${version}"
            ;;
        "ingest")
            image="difi/statistics-ingest-elasticsearch:${version}"
            ;;
        "authenticate")
            image="difi/statistics-authenticate:${version}"
            ;;
        *)
            fail "Unknown service ${service}"
    esac
    echo ${image}
}

serviceArgs() {
    local service=$1
    requireArgument 'service'
    case "${service}" in
        "elasticsearch")
            echo -n "-Ediscovery.zen.ping.unicast.hosts=elasticsearch_gossip:9301 -Enode.master=false"
            ;;
        "elasticsearch_gossip")
            echo -n "-Etransport.tcp.port=9301 -Enode.data=false"
            ;;
    esac
}

serviceAvailabilityUrl() {
    local service=$1
    requireArgument 'service'
    local host=${2-'localhost'}
    case "${service}" in
        'elasticsearch_gossip')
            echo -n "http://${host}:9201"
            ;;
        'elasticsearch')
            echo -n "http://${host}:8082"
            ;;
        'query')
            echo -n "http://${host}:8080/health"
            ;;
        'ingest')
            echo -n "http://${host}:8081/health"
            ;;
        'authenticate')
            echo -n "http://${host}:8083/health"
            ;;
        *)
            return 1
    esac
}

createService() {
    local service=$1
    local version=${2-'latest'}
    requireArgument 'service'
    local network='statistics'
    echo -n "Creating service ${service} with version ${version}: "
    local image=$(image ${service} ${version})
    local serviceArgs=$(serviceArgs ${service})
    local start=$SECONDS
    case ${service} in
    elasticsearch_gossip)
        output=$(sudo docker service create \
            --network ${network} \
            --constraint 'node.role == manager' \
            --stop-grace-period 5m \
            --name ${service} \
            -p 9201:9200 -p 9301:9301 \
            ${image} ${serviceArgs}) \
            || fail "Failed to create service ${service}"
        ;;
    elasticsearch)
        output=$(sudo docker service create \
            --network ${network} \
            --mode global \
            --stop-grace-period 5m \
            --name ${service} \
            --mount type=bind,src=/usr/share/elasticsearch/data,target=/usr/share/elasticsearch/data \
            -p 8082:9200 -p 9300:9300 \
            ${image} ${serviceArgs}) \
            || fail "Failed to create service ${service}"
        ;;
    query)
        output=$(sudo docker service create \
            --network ${network} \
            --mode global \
            --name ${service} \
            -p 8080:8080 \
            ${image} ${serviceArgs}) \
            || fail "Failed to create service ${service}"
        ;;
    ingest)
        output=$(sudo docker service create \
            --network ${network} \
            --mode global \
            --name ${service} \
            -p 8081:8080 \
            ${image} ${serviceArgs}) \
            || fail "Failed to create service ${service}"
        ;;
    authenticate)
        output=$(sudo docker service create \
            --network ${network} \
            --mode global \
            --name ${service} \
            -p 8083:8080 \
            ${image} ${serviceArgs}) \
            || fail "Failed to create service ${service}"
        ;;
    esac
    ok ${start}
}

updateService() {
    local service=$1
    local version=${2-'latest'}
    requireArgument 'service'
    echo -n "Updating service ${service} to version ${version}: "
    local image=$(image ${service} ${version})
    local serviceArgs=$(serviceArgs ${service})
    local start=$SECONDS
    output=$(sudo docker service inspect ${service}) || { echo "Service needs to be created"; createService ${service} ${version}; return; }
    output=$(sudo docker service update --image ${image} ${serviceArgs:+--args "${serviceArgs}"} ${service}) \
        && ok ${start} || fail
}

waitForServiceUpdateToComplete() {
    local service=$1
    requireArgument 'service'
    echo -n "Waiting for service \"${service}\" to be updated: "
    local start=$SECONDS
    waitFor "isServiceUpdateCompleted ${service}" 300 && ok ${start} || fail
}

isServiceUpdateCompleted() {
    local service="${1}"
    requireArgument 'service'
    updateStatus=$(sudo docker service inspect ${service} -f '{{.UpdateStatus.State}}')
    [ "${updateStatus}" == "completed" ] && return 0
    [ -z "${updateStatus}" ] && { echo "No update status found, assuming completed"; return 0; }
    [ "${updateStatus}" == "updating" ] && return 28
    [ "${updateStatus}" == "paused" ] && { echo "Update is paused, probably due to an error: $(sudo docker service inspect ${service} -f '{{.UpdateStatus.Message}}')"; return 1; }
    return 1
}

deleteService() {
    local service=$1
    requireArgument 'service'
    echo -n "Deleting service ${service}: "
    local start=$SECONDS
    output=$(sudo docker service rm ${service}) \
        && ok ${start} || fail
}

createNetwork() {
    local network=$1
    requireArgument 'network'
    echo -n "Creating network ${network}: "
    local start=$SECONDS
    output=$(sudo docker network create -d overlay --subnet 10.0.1.0/24 ${network}) \
        && ok ${start} || fail
}

deleteNetwork() {
    local network=$1
    requireArgument 'network'
    echo -n "Deleting network ${network}: "
    local start=$SECONDS
    output=$(sudo docker network rm ${network}) \
        && ok ${start} || fail
}

disableShardAllocation() {
    local host=${1-'localhost'}
    echo -n "Disabling Elasticsearch shard allocation: "
    local start=$SECONDS
    waitFor "doDisableShardAllocation ${host}" && ok ${start} || fail
}

enableShardAllocation() {
    local host=${1-'localhost'}
    echo -n "Enabling Elasticsearch shard allocation: "
    local start=$SECONDS
    waitFor "doEnableShardAllocation ${host}" && ok ${start} || fail
}

doDisableShardAllocation() {
    local host=${1-'localhost'}
    curl -s -f --connect-timeout 3 --max-time 10 \
        -XPUT http://${host}:8082/_cluster/settings -d '{"transient":{"cluster.routing.allocation.enable":"none"}}'
}

doEnableShardAllocation() {
    local host=${1-'localhost'}
    curl -s -f --connect-timeout 3 --max-time 10 \
        -XPUT http://${host}:8082/_cluster/settings -d '{"transient":{"cluster.routing.allocation.enable":"all"}}'
}

waitForGreenStatus() {
    local host=${1-'localhost'}
    echo -n "Waiting for Elasticsearch to be green... "
    local start=$SECONDS
    curl -s -f -XGET http://${host}:8082/_cluster/health?wait_for_status=green&timeout=5m > /dev/null \
        && ok ${start} || fail
}

indexExists() {
    local index=${1}
    requireArgument 'index'
    local host=${2-'localhost'}
    echo -n "Checking existence of index \"${index}\": "
    local start=$SECONDS
    waitFor "curl -s --connect-timeout 1 --max-time 1 -f http://${host}:8082/${index}/_search" && ok ${start} || fail
}

createTestData() {
    local host=${1-'localhost'}
    local user='991825827'
    echo -n "Creating credentials for user ${user}: "
    local start=$SECONDS
    local password
    password=$(waitFor "doCreateCredentials ${user} ${host}" 600) && ok ${start} || fail
    echo -n "Creating test data with credentials ${user}/${password}: "
    local start=$SECONDS
    waitFor "doCreateTestData ${user} ${password} ${host}" 600 && ok ${start} || fail
}

doCreateTestData() {
    local user="${1}"
    local password="${2}"
    requireArgument 'user'
    requireArgument 'password'
    local host=${3-'localhost'}
    local owner="${user}"
    curl \
        -s -f --connect-timeout 10 --max-time 600 \
        -f \
        -u "${user}":"${password}" \
        -H "Content-Type: application/json;charset=UTF-8" \
        -XPOST \
        http://${host}:8081/${owner}/test/minutes\?random\&from=2016-01-01T00:00:00.000Z\&to=2016-01-03T00:00:00.000Z
}

doCreateCredentials() {
    local user=$1
    requireArgument 'user'
    local host=${2-'localhost'}
    local password
    password=$(curl \
        -s -f --connect-timeout 3 --max-time 10 \
        -H "Content-Type: application/json;charset=UTF-8" \
        -XPOST \
        http://${host}:8083/credentials/${user}/short) || return $?
    echo -n ${password}
}

verifyTestData() {
    indexExists 991825827:test:minute2016.01.01 || return $?
    indexExists 991825827:test:minute2016.01.02 || return $?
}

waitForServiceToBeAvailable() {
    local service=$1
    requireArgument 'service'
    local host=${2-'localhost'}
    echo -n "Waiting for service \"${service}\" to be available: "
    local start=$SECONDS
    waitFor "isServiceAvailable ${service} ${host}" 100 && ok ${start} || fail
}

waitForServiceToBeUnavailable() {
    local service=$1
    requireArgument 'service'
    local host=${2-'localhost'}
    echo -n "Waiting for service \"${service}\" to be unavailable: "
    local start=$SECONDS
    waitFor "isServiceAvailable ${service} ${host}" 100 && ok ${start} || fail
}

isServiceAvailable() {
    local service=$1
    requireArgument 'service'
    local host=${2-'localhost'}
    local url
    url=$(serviceAvailabilityUrl ${service} ${host}) || return 1
    curl -s ${url} --connect-timeout 3 --max-time 10 > /dev/null
}

isServiceUnavailable() {
    local service=$1
    requireArgument 'service'
    local host=${2-'localhost'}
    local url
    url=$(serviceAvailabilityUrl ${service} ${host}) || return 1
    curl -s ${url} --connect-timeout 3 --max-time 10 > /dev/null
    ret=$?
    [ ${ret} -eq 7 ] && return 0
    [ ${ret} -eq 0 ] && return 1
    return ${ret}
}

create() {
    local version=${1-'latest'}
    echo "Creating application with version ${version}: "
    local start=$SECONDS
    createNetwork 'statistics' || return $?
    createService 'elasticsearch_gossip' ${version} || return $?
    waitForServiceToBeAvailable 'elasticsearch_gossip' || return $?
    createService 'elasticsearch' ${version} || return $?
    createService 'query' ${version} || return $?
    createService 'ingest' ${version} || return $?
    createService 'authenticate' ${version} || return $?
    echo "Application created ($(duration ${start})s)"
}

update() {
    local version=${1-'latest'}
    echo "Updating application to version ${version}: "
    local start=$SECONDS
    updateService 'query' ${version} || return $?
    updateService 'ingest' ${version} || return $?
    updateService 'authenticate' ${version} || return $?
    # Se https://www.elastic.co/guide/en/elasticsearch/reference/current/rolling-upgrades.html
    disableShardAllocation || return $?
    updateService 'elasticsearch' ${version} || return $?
    waitForServiceUpdateToComplete 'elasticsearch' || return $?
    waitForServiceToBeAvailable 'elasticsearch' || return $?
    updateService 'elasticsearch_gossip' ${version} || return $?
    waitForServiceUpdateToComplete 'elasticsearch_gossip' || return $?
    waitForServiceToBeAvailable 'elasticsearch_gossip' || return $?
    enableShardAllocation || return $?
    echo "Application updated ($(duration ${start})s)"
}

delete() {
    echo "Deleting application: "
    local start=$SECONDS
    deleteService "authenticate" || return $?
    deleteService "ingest" || return $?
    deleteService "query" || return $?
    deleteService "elasticsearch" || return $?
    deleteService "elasticsearch_gossip" || return $?
    deleteNetwork "statistics" || return $?
    echo "Application deleted ($(duration ${start})s)"
}

createAndVerify() {
    local version=${1-'latest'}
    create latest || return $?
    verify ${version} || return $?
}

verify() {
    local version=${1-'latest'}
    waitForServiceToBeAvailable 'elasticsearch' || return $?
    waitForServiceToBeAvailable 'authenticate' || return $?
    waitForServiceToBeAvailable 'ingest' || return $?
    createTestData || return $?
    waitForGreenStatus || return $?
    update ${version} || return $?
    waitForGreenStatus || return $?
    verifyTestData || return $?
    deleteService 'elasticsearch' || return $?
    deleteService 'elasticsearch_gossip' || return $?
    waitForServiceToBeUnavailable 'elasticsearch' || return $?
    waitForServiceToBeUnavailable 'elasticsearch_gossip' || return $?
    createService 'elasticsearch' || return $?
    createService 'elasticsearch_gossip' || return $?
    waitForServiceToBeAvailable 'elasticsearch_gossip' || return $?
    waitForServiceToBeAvailable 'elasticsearch' || return $?
    waitForGreenStatus || return $?
    verifyTestData || return $?
}

case "${1}" in *)
        function="${1}"
        shift
        ${function} "${@}"
        ;;
esac
