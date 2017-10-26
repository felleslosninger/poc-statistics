import java.time.ZoneId
import java.time.format.DateTimeFormatter
import org.jenkinsci.plugins.workflow.steps.FlowInterruptedException

import static java.time.ZonedDateTime.now

def verificationHostName = 'eid-test-docker01.dmz.local'
def verificationHostUser = 'jenkins'
def verificationHostSshKey = 'ssh.git.difi.local'
def productionHostName = 'eid-prod-docker01.dmz.local'
def productionHostUser = 'jenkins'
def productionHostSshKey = 'ssh.git.difi.local'
def gitSshKey = 'ssh.github.com'

pipeline {
    agent none
    options {
        timeout(time: 5, unit: 'DAYS')
        disableConcurrentBuilds()
        ansiColor('xterm')
        timestamps()
    }
    stages {
        stage('Check build') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) } }
            agent any
            steps {
                transitionIssue env.ISSUE_STATUS_OPEN, env.ISSUE_TRANSITION_START
                ensureIssueStatusIs env.ISSUE_STATUS_IN_PROGRESS
                script {
                    currentBuild.description = "Building from commit " + readCommitId()
                    env.MAVEN_OPTS = readProperties(file: 'Jenkinsfile.properties').MAVEN_OPTS
                    if (readCommitMessage() == "ready!") {
                        env.verification = 'true'
                    }
                }
                sh "mvn clean verify -B"
            }
        }
        stage('Wait for verification to start') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            steps {
                transitionIssue env.ISSUE_TRANSITION_READY_FOR_CODE_REVIEW
                waitUntilIssueStatusIs env.ISSUE_STATUS_CODE_REVIEW
            }
        }
        stage('Wait for verification slot') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            agent any
            steps {
                failIfJobIsAborted()
                sshagent([gitSshKey]) {
                    retry(count: 1000000) {
                        sleep 10
                        sh 'pipeline/git/available-verification-slot'
                    }
                }
            }
            post {
                failure { transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK }
                aborted { transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK }
            }
        }
        stage('Prepare verification') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            environment {
                crucible = credentials('crucible')
            }
            agent any
            steps {
                script {
                    env.version = DateTimeFormatter.ofPattern('yyyy-MM-dd-HHmm').format(now(ZoneId.of('UTC'))) + "-" + readCommitId()
                    commitMessage = "${env.version}|" + issueId() + ": " + issueSummary()
                    sshagent([gitSshKey]) {
                        verifyRevision = sh returnStdout: true, script: "pipeline/git/create-verification-revision \"${commitMessage}\""
                    }
                    sh "pipeline/create-review ${verifyRevision} ${env.crucible_USR} ${env.crucible_PSW}"
                }
            }
            post {
                failure {
                    deleteVerificationBranch()
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                }
                aborted {
                    deleteVerificationBranch()
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                }
            }
        }
        stage('Build artifacts') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            environment {
                nexus = credentials('nexus')
                artifactory = credentials('artifactory-publish')
            }
            agent any
            steps {
                script {
                    checkoutVerificationBranch()
                    currentBuild.description = "Building ${env.version} from commit " + readCommitId()
                    env.MAVEN_OPTS = readProperties(file: 'Jenkinsfile.properties').MAVEN_OPTS
                    sh "mvn versions:set -B -DnewVersion=${env.version}"
                    sh """
                       mvn deploy -B -s settings.xml \
                       -Dmaven.release.username=${env.artifactory_USR} \
                       -Dmaven.release.password=${env.artifactory_PSW} \
                       -Ddocker.release.username=${env.nexus_USR} \
                       -Ddocker.release.password=${env.nexus_PSW} \
                       -DdeployAtEnd=true
                       """
                }
            }
            post {
                failure {
                    deleteVerificationBranch()
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                }
                aborted {
                    deleteVerificationBranch()
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                }
            }
        }
        stage('Deploy for verification') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            environment {
                aws = credentials('aws')
            }
            agent {
                dockerfile {
                    dir 'docker'
                    args '-v /var/jenkins_home/.ssh/known_hosts:/root/.ssh/known_hosts -u root:root'
                }
            }
            steps {
                script {
                    env.AWS_DEFAULT_REGION = 'us-east-1'
                    env.AWS_ACCESS_KEY_ID = env.aws_USR
                    env.AWS_SECRET_ACCESS_KEY = env.aws_PSW
                    checkoutVerificationBranch()
                    node1 = "statistics-${env.version}-node1"
                    node2 = "statistics-${env.version}-node2"
                    //sh "pipelinex/environment.sh create ${env.version}"
                    //sh "pipelinex/environment.sh login ${node1} bash -s -- < pipelinex/application.sh create latest" // TODO: Should be previous version
                }
            }
            post {
                failure {
                    deleteVerificationBranch()
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                    sh "pipelinex/environment.sh delete ${version}"
                }
                aborted {
                    deleteVerificationBranch()
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                    sh "pipelinex/environment.sh delete ${version}"
                }
            }
        }
        stage('Verify behaviour') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            agent {
                dockerfile {
                    dir 'docker'
                    args '-v /var/jenkins_home/.ssh/known_hosts:/root/.ssh/known_hosts -u root:root'
                }
            }
            steps {
                script {
                    checkoutVerificationBranch()
                    node1 = "statistics-${env.version}-node1"
                    node2 = "statistics-${env.version}-node2"
                    //sh "pipelinex/environment.sh login ${node1} bash -s -- < pipelinex/application.sh verify ${env.version}"
                    //sh "pipelinex/environment.sh terminateNode ${node1}"
                    //sh "pipelinex/environment.sh login ${node2} bash -s -- < pipelinex/application.sh verifyTestData"
                }
            }
            post {
//                always {
//                    sh "pipelinex/environment.sh delete ${env.version}"
//                }
                failure {
                    deleteVerificationBranch()
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                }
                aborted {
                    deleteVerificationBranch()
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                }
            }
        }
        stage('Wait for code review to finish') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            steps {
                waitUntilIssueStatusIsNot env.ISSUE_STATUS_CODE_REVIEW
                script {
                    env.codeApproved = "false"
                    if (issueStatusIs(env.ISSUE_STATUS_CODE_APPROVED))
                        env.codeApproved = "true"
                }
            }
        }
        stage('Integrate code') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            agent any
            steps {
                failIfJobIsAborted()
                script {
                    checkoutVerificationBranch()
                    sshagent([gitSshKey]) {
                        sh 'git push origin HEAD:master'
                    }
                }
            }
            post {
                always {
                    deleteVerificationBranch()
                }
                success {
                    deleteWorkBranch()
                }
                failure {
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                }
                aborted {
                    transitionIssue env.ISSUE_STATUS_CODE_REVIEW, env.ISSUE_TRANSITION_RESUME_WORK
                }
            }
        }
        stage('Wait for manual verification to start') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            steps {
                waitUntilIssueStatusIs env.ISSUE_STATUS_MANUAL_VERIFICATION
            }
        }
        stage('Deploy for manual verification') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            agent any
            steps {
                failIfJobIsAborted()
                script {
                    currentBuild.description = "Deploying ${env.version} to verification environment"
                    sshagent([verificationHostSshKey]) {
                        sh "ssh -o StrictHostKeyChecking=no ${verificationHostUser}@${verificationHostName} bash -s -- < pipelinex/application.sh update ${env.version}"
                    }
                }
            }
        }
        stage('Wait for manual verification to finish') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            steps {
                waitUntilIssueStatusIsNot env.ISSUE_STATUS_MANUAL_VERIFICATION
                failIfJobIsAborted()
                ensureIssueStatusIs env.ISSUE_STATUS_MANUAL_VERIFICATION_OK
            }
        }
        stage('Deploy for production') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            agent any
            steps {
                failIfJobIsAborted()
                script {
                    currentBuild.description = "Deploying ${env.version} to production environment"
                    sshagent([productionHostSshKey]) {
                        sh "ssh -o StrictHostKeyChecking=no ${productionHostUser}@${productionHostName} bash -s -- < pipelinex/application.sh update ${env.version}"
                    }
                }
            }
        }
    }
    post {
        success {
            echo "Success"
            notifySuccess()
        }
        unstable {
            echo "Unstable"
            notifyUnstable()
        }
        failure {
            echo "Failure"
            notifyFailed()
        }
        aborted {
            echo "Aborted"
            notifyFailed()
        }
        always {
            echo "Build finished"
        }
    }
}

def checkoutVerificationBranch() {
    sh "git checkout verify/\${BRANCH_NAME}"
    sh "git reset --hard origin/verify/\${BRANCH_NAME}"
}

def deleteVerificationBranch() {
    echo "Deleting verification branch"
    sshagent([gitSshKey]) { sh "git push origin --delete verify/\${BRANCH_NAME}" }
    echo "Verification branch deleted"
}

def deleteWorkBranch() {
    sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME}" }
}

def failIfJobIsAborted() {
    if (env.jobAborted == 'true')
        error('Job was aborted')
}

boolean issueStatusIs(def targetStatus) {
    return issueStatus() == targetStatus
}

def waitUntilIssueStatusIs(def targetStatus) {
    env.jobAborted = 'false'
    try {
        retry(count: 1000000) {
            if (!issueStatusIs(targetStatus)) {
                sleep 10
                error "Waiting until issue status is ${targetStatus}..."
            }
        }
    } catch (FlowInterruptedException e) {
        env.jobAborted = "true"
    }
}

def waitUntilIssueStatusIsNot(def targetStatus) {
    env.jobAborted = 'false'
    try {
        retry(count: 1000000) {
            if (issueStatusIs(targetStatus)) {
                sleep 10
                error "Waiting until issue status is not ${targetStatus}..."
            }
        }
    } catch (FlowInterruptedException e) {
        env.jobAborted = "true"
    }
}

def notifyFailed() {
    emailext (
            subject: "FAILED: '${env.JOB_NAME}'",
            body: """<p>FAILED: Bygg '${env.JOB_NAME} [${env.BUILD_NUMBER}]' feilet.</p>
            <p><b>Konsoll output:</b><br/>
            <a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a></p>""",
            recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']]
    )
}

def notifyUnstable() {
    emailext (
            subject: "UNSTABLE: '${env.JOB_NAME}'",
            body: """<p>UNSTABLE: Bygg '${env.JOB_NAME} [${env.BUILD_NUMBER}]' er ustabilt.</p>
            <p><b>Konsoll output:</b><br/>
            <a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a></p>""",
            recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']]
    )
}

def notifySuccess() {
    if (isPreviousBuildFailOrUnstable()) {
        emailext (
                subject: "SUCCESS: '${env.JOB_NAME}'",
                body: """<p>SUCCESS: Bygg '${env.JOB_NAME} [${env.BUILD_NUMBER}]' er oppe og snurrer igjen.</p>""",
                recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']]
        )
    }
}

boolean isPreviousBuildFailOrUnstable() {
    if(!hudson.model.Result.SUCCESS.equals(currentBuild.rawBuild.getPreviousBuild()?.getResult())) {
        return true
    }
    return false
}

def issueId() {
    return env.BRANCH_NAME.tokenize('/')[-1]
}

def transitionIssue(def transitionId) {
    jiraTransitionIssue idOrKey: issueId(), input: [transition: [id: transitionId]]
}

def transitionIssue(def sourceStatus, def transitionId) {
    if (issueStatusIs(sourceStatus))
        transitionIssue transitionId
}

def ensureIssueStatusIs(def issueStatus) {
    if (!issueStatusIs(issueStatus))
        error "Issue status is not ${issueStatus}"
}

String issueStatus() {
    return jiraGetIssue(idOrKey: issueId()).data.fields['status']['id']
}

String issueSummary() {
    return jiraGetIssue(idOrKey: issueId()).data.fields['summary']
}

def readCommitId() {
    return sh(returnStdout: true, script: 'git rev-parse HEAD').trim().take(7)
}

def readCommitMessage() {
    return sh(returnStdout: true, script: 'git log -1 --pretty=%B').trim()
}
