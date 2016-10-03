#!groovy​
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

String version = DateTimeFormatter.ofPattern('yyyyMMddHHmm').format(ZonedDateTime.now(ZoneId.of('UTC')))

stage 'Build'
node {
    checkout scm
    if (env.BRANCH_NAME == 'develop') {
        sh "mvn clean versions:set -DnewVersion=${version}"
        sh 'mvn deploy'
    } else {
        sh 'mvn clean verify'
    }
}

if (env.BRANCH_NAME == 'develop') {

    stage 'Staging deploy'
    node {
        sh "ssh eid-test-docker01 docker service update --image difi/statistics-query-elasticsearch:${version} statistics-query"
        sh "ssh eid-test-docker01 docker service update --image difi/statistics-ingest-elasticsearch:${version} statistics-ingest"
        sh "ssh eid-test-docker01 docker service update --image difi/statistics-elasticsearch:${version} elasticsearch"
        sh "ssh eid-test-docker01 docker service update --image difi/statistics-elasticsearch:${version} elasticsearch_gossip"
    }

    stage 'Production deploy'
    timeout(time:5, unit:'DAYS') {
        input "Do you approve deployment of version ${version} to production?"
        node {
            sh "ssh eid-prod-docker01 docker service update --image difi/statistics-query-elasticsearch:${version} statistics-query"
            sh "ssh eid-prod-docker01 docker service update --image difi/statistics-ingest-elasticsearch:${version} statistics-ingest"
            sh "ssh eid-prod-docker01 docker service update --image difi/statistics-elasticsearch:${version} elasticsearch"
            sh "ssh eid-prod-docker01 docker service update --image difi/statistics-elasticsearch:${version} elasticsearch_gossip"
        }
    }

}