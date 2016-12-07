#!/usr/bin/env bash

fail() {
    ret=$?
    message=${1-"[Failed (${ret})]"}
    echo ${message}
    return ${ret}
}

warn() {
    message=$1
    echo ${message}
}

ok() {
    echo "[OK]"
}

dotSleep() {
    length=${1-1}
    echo -n "."
    sleep ${length};
}

requireArgument() {
    test -z ${!1} && fail "Missing argument '${1}'"
}

image() {
    service=$1
    version=${2-'latest'}
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

createService() {
    service=$1
    version=${2-'latest'}
    requireArgument 'service'
    network='statistics'
    echo -n "Creating service ${service} of version ${version}... "
    image=$(image ${service} ${version})
    case ${service} in
    elasticsearch_gossip)
        output=$(sudo docker service create \
            --network ${network} \
            --constraint 'node.role == manager' \
            --stop-grace-period 5m \
            --name ${service} \
            -p 9201:9200 -p 9301:9301 \
            ${image} -Des.transport.tcp.port=9301 -Des.node.data=false) \
            || fail "Failed to create service ${service}"
        ;;
    elasticsearch)
        output=$(sudo docker service create \
            --network ${network} \
            --mode global \
            --stop-grace-period 5m \
            --name ${service} \
            -p 8082:9200 -p 9300:9300 \
            ${image} -Des.discovery.zen.ping.unicast.hosts=elasticsearch_gossip:9301 -Des.node.master=false) \
            || fail "Failed to create service ${service}"
        ;;
    query)
        output=$(sudo docker service create \
            --network ${network} \
            --mode global \
            --name ${service} \
            -p 8080:8080 \
            ${image}) \
            || fail "Failed to create service ${service}"
        ;;
    ingest)
        output=$(sudo docker service create \
            --network ${network} \
            --mode global \
            --name ${service} \
            -p 8081:8080 \
            ${image}) \
            || fail "Failed to create service ${service}"
        ;;
    authenticate)
        output=$(sudo docker service create \
            --network ${network} \
            --mode global \
            --name ${service} \
            -p 8083:8080 \
            ${image}) \
            || fail "Failed to create service ${service}"
        ;;
    esac
    ok
}

updateService() {
    service=$1
    version=${2-'latest'}
    requireArgument 'service'
    echo -n "Updating service ${service} to version ${version}... "
    image=$(image ${service} ${version})
    output=$(sudo docker service inspect ${service}) || { echo "Service needs to be created"; createService ${service} ${version}; return; }
    output=$(sudo docker service update --image ${image} ${service}) \
        && ok || fail
}

waitForServiceUpdateToComplete() {
    service=$1
    requireArgument 'service'
    echo -n "Waiting for service \"${service}\" to be updated"
    for i in $(seq 1 100); do isServiceUpdateCompleted ${service} && status=true && break || dotSleep; done
    echo -n " "
    ${status} && ok || fail
}

isServiceUpdateCompleted() {
    service="${1}"
    requireArgument 'service'
    [ "$(sudo docker service inspect ${service} -f '{{.UpdateStatus.State}}')" == "completed" ]
}

deleteService() {
    service=$1
    requireArgument 'service'
    echo -n "Deleting service ${service}... "
    output=$(sudo docker service rm ${service}) \
        && ok || fail
}

createNetwork() {
    network=$1
    requireArgument 'network'
    echo -n "Creating network ${network}... "
    output=$(sudo docker network create -d overlay --subnet 10.0.1.0/24 ${network}) \
        && ok || fail
}

deleteNetwork() {
    network=$1
    requireArgument 'network'
    echo -n "Deleting network ${network}... "
    output=$(sudo docker network rm ${network}) \
        && ok || fail
}

disableShardAllocation() {
    host=${1-'localhost'}
    echo -n "Disabling Elasticsearch shard allocation..."
    curl -s -XPUT http://${host}:8082/_cluster/settings \
        -d '{"transient":{"cluster.routing.allocation.enable":"none"}}' \
        && ok || fail
}

enableShardAllocation() {
    host=${1-'localhost'}
    echo -n "Enabling Elasticsearch shard allocation..."
    curl -s -XPUT http://${host}:8082/_cluster/settings \
        -d '{"transient":{"cluster.routing.allocation.enable":"all"}}' \
        && ok || fail
}

waitForGreenStatus() {
    host=${1-'localhost'}
    echo -n "Waiting for Elasticsearch to be green... "
    curl -sS -f -XGET http://${host}:8082/_cluster/health?wait_for_status=green&timeout=5m > /dev/null \
        && ok || fail
}

indexExists() {
    index=${1}
    host=${2-'localhost'}
    echo -n "Checking existence of index \"${index}\"... "
    curl -sS -f --head http://${host}:8082/${index} > /dev/null \
        && ok || fail
}

createTestData() {
    host=${1-'localhost'}
    user='991825827'
    echo -n "Creating credentials for user ${user}..."
    for i in $(seq 1 600); do
        password=$(doCreateCredentials ${user} ${host})
        ret=$?
        [ ! ${ret} -eq 7 -a ! ${ret} -eq 22 ] && break # Stop retry if neither connect error nor server failure
        dotSleep
    done
    [ ${ret} -eq 0 ] && ok || fail
    echo -n "Creating test data with credentials ${user}/${password}... "
    for i in $(seq 1 600); do
        doCreateTestData "${user}" "${password}" "${host}"
        ret=$?
        [ ! ${ret} -eq 7 -a ! ${ret} -eq 22 ] && break # Stop retry if neither connect error nor server failure
        dotSleep
    done
    [ ${ret} -eq 0 ] && ok || fail
}

doCreateTestData() {
    user="${1}"
    password="${2}"
    requireArgument 'user'
    requireArgument 'password'
    host=${3-'localhost'}
    owner="${user}"
    curl \
        -sS \
        -f \
        -u "${user}":"${password}" \
        -H "Content-Type: application/json;charset=UTF-8" \
        -XPOST \
        http://${host}:8081/${owner}/test/minutes\?random\&from=2016-01-01T00:00:00.000Z\&to=2016-01-03T00:00:00.000Z
}

doCreateCredentials() {
    user=$1
    requireArgument 'user'
    host=${2-'localhost'}
    password=$(curl \
        -sS \
        -f \
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
    service=$1
    requireArgument 'service'
    host=${2-'localhost'}
    echo -n "Waiting for service \"${service}\" to be available: "
    status=false
    for i in $(seq 1 200); do
        isServiceAvailable ${service} ${host}
        ret=$?
        [ ${ret} -eq 7 -o ${ret} -eq 27 ] && dotSleep; # Connect failure or request timeout
        [ ${ret} -eq 0 ] && { status=true; break; }
        [ ${ret} -eq 1 ] && break # Unknown service
    done
    ${status} && ok || fail
}

isServiceAvailable() {
    service=$1
    requireArgument 'service'
    host=${2-'localhost'}
    case "${service}" in
        'elasticsearch_gossip')
            url="http://${host}:9201"
            ;;
        'elasticsearch')
            url="http://${host}:8082"
            ;;
        'query')
            url="http://${host}:8080/health"
            ;;
        'ingest')
            url="http://${host}:8081/health"
            ;;
        'authenticate')
            url="http://${host}:8083/health"
            ;;
        *)
            echo -n "Unknown service \"${service}\""
            return 1
    esac
    curl -s ${url} --connect-timeout 3 --max-time 10 > /dev/null
}

create() {
    version=${1-'latest'}
    echo "Creating application with version ${version}..."
    createNetwork 'statistics' || return $?
    createService 'elasticsearch_gossip' ${version} || return $?
    waitForServiceToBeAvailable 'elasticsearch_gossip' || return $?
    createService 'elasticsearch' ${version} || return $?
    createService 'query' ${version} || return $?
    createService 'ingest' ${version} || return $?
    createService 'authenticate' ${version} || return $?
    echo "Application created"
}

update() {
    version=${1-'latest'}
    echo "Updating application to version ${version}..."
    updateService 'query' ${version} || return $?
    updateService 'ingest' ${version} || return $?
    updateService 'authenticate' ${version} || return $?
    # Se https://www.elastic.co/guide/en/elasticsearch/reference/current/rolling-upgrades.html
    # Inntil data persisteres (utenfor konteiner), så må shards reallokeres under oppgradering for at de skal beholdes.
    #disableShardAllocation
    updateService 'elasticsearch' ${version} || return $?
    waitForServiceUpdateToComplete 'elasticsearch' || return $?
    waitForServiceToBeAvailable 'elasticsearch' || return $?
    updateService 'elasticsearch_gossip' ${version} || return $?
    waitForServiceUpdateToComplete 'elasticsearch_gossip' || return $?
    waitForServiceToBeAvailable 'elasticsearch_gossip' || return $?
    #enableShardAllocation
    echo "Application updated"
}

delete() {
    echo "Deleting application..."
    deleteService "authenticate"
    deleteService "ingest"
    deleteService "query"
    deleteService "elasticsearch"
    deleteService "elasticsearch_gossip"
    deleteNetwork "statistics"
    echo "Application deleted"
}

createAndVerify() {
    version=${1-'latest'}
    create latest || return $?
    verify ${version} || return $?
}

verify() {
    version=${1-'latest'}
    waitForServiceToBeAvailable 'elasticsearch' || return $?
    waitForServiceToBeAvailable 'ingest' || return $?
    waitForServiceToBeAvailable 'authenticate' || return $?
    createTestData || return $?
    waitForGreenStatus || return $?
    update ${version} || return $?
    waitForGreenStatus || return $?
    verifyTestData || return $?
}

case "${1}" in *)
        function="${1}"
        shift
        ${function} "${@}"
        ;;
esac
