node {
    checkout scm
    if (env.BRANCH_NAME == "develop") {
        String version = java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmm").format(java.time.ZonedDateTime.now(java.time.ZoneId.of("UTC")))
        sh "mvn clean versions:set -DnewVersion=${version}"
        sh "mvn deploy"
        sh "ssh eid-test-docker01.dmz.local docker service update --image difi/statistics-query-elasticsearch:${version} statistics-query"
        sh "ssh eid-test-docker01.dmz.local docker service update --image difi/statistics-ingest-elasticsearch:${version} statistics-ingest"
        sh "ssh eid-test-docker01.dmz.local docker service update --image difi/statistics-elasticsearch:${version} elasticsearch"
        sh "ssh eid-test-docker01.dmz.local docker service update --image difi/statistics-elasticsearch:${version} elasticsearch_gossip"
    } else {
        sh "mvn clean verify"
    }
}
