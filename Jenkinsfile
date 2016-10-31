#!groovy
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import static java.time.ZonedDateTime.now

String version = DateTimeFormatter.ofPattern('yyyyMMddHHmm').format(now(ZoneId.of('UTC')))
String deployBranch = 'master'
String qaFeatureBranch = /feature\/qa\/(\w+-\w+)/
String featureBranch = /feature\/(\w+-\w+)/

stage('Build') {

    node {
        checkout scm
        def commitId = commitId()
        stash includes: 'pipeline/*', name: 'pipeline'
        if (env.BRANCH_NAME.matches(deployBranch)) {
            currentBuild.displayName = "#${currentBuild.number}: Deploy version ${version}"
            currentBuild.description = "Commit: ${commitId}"
            sh "pipeline/build.sh deliver ${version}"
        } else if (env.BRANCH_NAME.matches(qaFeatureBranch)) {
            jiraId = (env.BRANCH_NAME =~ qaFeatureBranch)[0][1]
            currentBuild.displayName = "#${currentBuild.number}: QA for feature ${jiraId}"
            currentBuild.description = "Feature: ${jiraId} Commit: ${commitId}"
            sh "pipeline/build.sh deliver ${version}"
        } else if (env.BRANCH_NAME.matches(featureBranch)) {
            jiraId = (env.BRANCH_NAME =~ featureBranch)[0][1]
            currentBuild.displayName = "#${currentBuild.number}: Build for feature ${jiraId}"
            currentBuild.description = "Feature: ${jiraId} Commit: ${commitId}"
            sh "pipeline/build.sh verify"
        }
    }

}

if (env.BRANCH_NAME.matches(qaFeatureBranch)) {

    stage('QA') {
        node {
            unstash 'pipeline'
            sh "pipeline/environment.sh create ${version}"
            managerNode = "statistics-${version}-node01"
            sh "docker-machine ssh ${managerNode} bash -s -- < pipeline/application.sh createAndVerify ${version}"
            sh "pipeline/environment.sh delete ${version}"
        }
    }

}

if (env.BRANCH_NAME.matches(deployBranch)) {

    stage('Staging deploy') {
        node {
            unstash 'pipeline'
            sh "ssh 'eid-test-docker01.dmz.local' bash -s -- < pipeline/application.sh update ${version}"
        }
    }

    stage('Production deploy') {
        timeout(time: 5, unit: 'DAYS') {
            input "Do you approve deployment of version ${version} to production?"
            node {
                unstash 'pipeline'
                sh "ssh 'eid-prod-docker01.dmz.local' bash -s -- < pipeline/application.sh update ${version}"
            }
        }
    }

}

def commitId() {
    sh 'git rev-parse HEAD > commit'
    return readFile('commit').trim()
}

