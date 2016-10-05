#!groovy​
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

String version = DateTimeFormatter.ofPattern('yyyyMMddHHmm').format(ZonedDateTime.now(ZoneId.of('UTC')))
String deployBranch = 'develop'

stage('Build') {

    node {
        checkout scm
        stash includes: 'pipeline/*.sh', name: 'pipeline'
        if (env.BRANCH_NAME == deployBranch) {
            sh "pipeline/buildAndDeliver.sh ${version}"
        } else {
            sh "pipeline/buildAndVerify.sh"
        }
    }

}

if (env.BRANCH_NAME == deployBranch) {

    stage('Staging deploy') {

        node {
            unstash 'pipeline'
            upgrade('eid-test-docker01', "${version}")
        }
    }

    stage('Production deploy') {
        timeout(time: 5, unit: 'DAYS') {
            input "Do you approve deployment of version ${version} to production?"
            node {
                unstash 'pipeline'
                upgrade('eid-prod-docker01', "${version}")
            }
        }

    }

}

def upgrade(String masterNode, String version) {
    updateService(masterNode, 'statistics-query', 'difi/statistics-query-elasticsearch', version, 10)
    updateService(masterNode, 'statistics-ingest', 'difi/statistics-ingest-elasticsearch', version, 10)
    // See https://www.elastic.co/guide/en/elasticsearch/reference/current/rolling-upgrades.html
    disableShardAllocation(masterNode)
    updateService(masterNode, 'elasticsearch', 'difi/statistics-elasticsearch', version, 200)
    updateService(masterNode, 'elasticsearch_gossip', 'difi/statistics-elasticsearch', version, 200)
    enableShardAllocation(masterNode)
}

def updateService(String masterNode, String service, String image, String version, int delay) {
    sh "pipeline/updateService.sh ${service} ${image} ${version} ${masterNode} ${delay}"
}

def disableShardAllocation(String masterNode) {
    sh "pipeline/disableShardAllocation.sh ${masterNode}"
}

def enableShardAllocation(String masterNode) {
    sh "pipeline/enableShardAllocation.sh ${masterNode}"
}
