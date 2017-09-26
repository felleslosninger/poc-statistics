import java.time.ZoneId
import java.time.format.DateTimeFormatter
import org.jenkinsci.plugins.workflow.steps.FlowInterruptedException

import static java.time.ZonedDateTime.now

def verificationDeployHostName = 'eid-test01.dmz.local'
def verificationDeployHostUser = 'jenkins'
def productionDeployHostName = 'eid-prod01.dmz.local'
def productionDeployHostUser = 'jenkins'
def gitSshKey = 'ssh.github.com'

pipeline {
    agent none
    options {
        timeout(time: 5, unit: 'DAYS')
        disableConcurrentBuilds()
        ansiColor('xterm')
    }
    stages {
        stage('Check build') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) } }
            agent any
            steps {
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
        stage('Wait for code reviewer to start') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            steps {
                script {
                    retry(count: 1000000) {
                        if (issueStatus(issueId(env.BRANCH_NAME)) != env.ISSUE_STATUS_CODE_REVIEW) {
                            sleep 10
                            error("Issue is not yet under code review")
                        }
                    }
                }
            }
        }
        stage('Wait for verification slot') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            agent any
            steps {
                script {
                    sshagent([gitSshKey]) {
                        retry(count: 1000000) {
                            sleep 10
                            sh 'pipeline/git/available-verification-slot'
                        }
                    }
                }
            }
        }
        stage('Create code review') {
            when { expression { env.BRANCH_NAME.matches(/(work|feature|bugfix)\/(\w+-\w+)/) && env.verification == 'true' } }
            environment {
                crucible = credentials('crucible')
            }
            agent any
            steps {
                script {
                    version = DateTimeFormatter.ofPattern('yyyy-MM-dd-HHmm').format(now(ZoneId.of('UTC'))) + "-" + readCommitId()
                    sshagent([gitSshKey]) {
                        verifyRevision = sh returnStdout: true, script: "pipeline/git/create-verification-revision ${version}"
                    }
                    sh "pipeline/create-review ${verifyRevision} ${env.crucible_USR} ${env.crucible_PSW}"
                }
            }
            post {
                failure { sshagent([gitSshKey]) { sh "git push origin --delete verify/\${BRANCH_NAME}" }}
                aborted { sshagent([gitSshKey]) { sh "git push origin --delete verify/\${BRANCH_NAME}" }}
            }
        }
        stage('Build artifacts') {
            when { expression { env.BRANCH_NAME.matches(/verify\/(work|feature|bugfix)\/(\w+-\w+)/) } }
            environment {
                nexus = credentials('nexus')
            }
            agent any
            steps {
                script {
                    version = versionFromCommitMessage()
                    currentBuild.description = "Building ${version} from commit " + readCommitId()
                    env.MAVEN_OPTS = readProperties(file: 'Jenkinsfile.properties').MAVEN_OPTS
                    sh "mvn versions:set -B -DnewVersion=${version}"
                    sh "mvn deploy -B -DdeployAtEnd=true"
                }
            }
            post {
                failure { sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME}" }}
                aborted { sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME}" }}
            }
        }
        stage('Deploy for verification') {
            when { expression { env.BRANCH_NAME.matches(/verify\/(work|feature|bugfix)\/(\w+-\w+)/) } }
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
                    version = versionFromCommitMessage()
                    node1 = "statistics-${version}-node1"
                    node2 = "statistics-${version}-node2"
                    sh "pipelinex/environment.sh create ${version}"
                    sh "pipelinex/environment.sh login ${node1} bash -s -- < pipelinex/application.sh create latest" // TODO: Should be previous version
                }
            }
            post {
                failure {
                    sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME}" }
                    sh "pipelinex/environment.sh delete ${version}"
                }
                aborted {
                    sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME}" }
                    sh "pipelinex/environment.sh delete ${version}"
                }
            }
        }
        stage('Verify behaviour') {
            when { expression { env.BRANCH_NAME.matches(/verify\/(work|feature|bugfix)\/(\w+-\w+)/) } }
            agent {
                dockerfile {
                    dir 'docker'
                    args '-v /var/jenkins_home/.ssh/known_hosts:/root/.ssh/known_hosts -u root:root'
                }
            }
            steps {
                script {
                    version = versionFromCommitMessage()
                    node1 = "statistics-${version}-node1"
                    node2 = "statistics-${version}-node2"
                    sh "pipelinex/environment.sh login ${node1} bash -s -- < pipelinex/application.sh verify ${version}"
                    sh "pipelinex/environment.sh terminateNode ${node1}"
                    sh "pipelinex/environment.sh login ${node2} bash -s -- < pipelinex/application.sh verifyTestData"
                }
            }
            post {
                always {
                    sh "pipelinex/environment.sh delete ${version}"
                }
                failure { sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME}" }}
                aborted { sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME}" }}
            }
        }
        stage('Wait for code reviewer to finish') {
            when { expression { env.BRANCH_NAME.matches(/verify\/(work|feature|bugfix)\/(\w+-\w+)/) } }
            steps {
                script {
                    env.codeApproved = "false"
                    env.jobAborted = "false"
                    try {
                        retry(count: 1000000) {
                            if (issueStatus(issueId(env.BRANCH_NAME)) == env.ISSUE_STATUS_CODE_REVIEW) {
                                sleep 10
                                error("Issue is still under code review")
                            }
                        }
                        if (issueStatus(issueId(env.BRANCH_NAME)) == env.ISSUE_STATUS_CODE_APPROVED)
                            env.codeApproved = "true"
                    } catch (FlowInterruptedException e) {
                        env.jobAborted = "true"
                    }
                }
            }
        }
        stage('Integrate code') {
            when { expression { env.BRANCH_NAME.matches(/verify\/(work|feature|bugfix)\/(\w+-\w+)/) } }
            agent any
            steps {
                script {
                    sshagent([gitSshKey]) {
                        sh 'git push origin HEAD:master'
                    }
                }
            }
            post {
                always {
                    sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME}" }
                }
                success {
                    sshagent([gitSshKey]) { sh "git push origin --delete \${BRANCH_NAME#verify/}" }
                }
            }
        }
        stage('Wait for tester to start') {
            when { branch 'master' }
            steps {
                script {
                    env.jobAborted = 'false'
                    try {
                        input message: "Ready to perform manual behaviour verification?", ok: "Yes"
                    } catch (Exception ignored) {
                        env.jobAborted = 'true'
                    }
                }
            }
        }
        stage('Deploy for manual verification') {
            when { branch 'master' }
            agent any
            steps {
                script {
                    if (env.jobAborted == 'true') {
                        error('Job was aborted')
                    }
                    version = versionFromCommitMessage()
                    currentBuild.description = "Deploying ${version} to manual verification environment"
                    sh "ssh ${verificationDeployHostUser}@${verificationDeployHostName} bash -s -- < pipelinex/application.sh update ${version}"
                }
            }
        }
        stage('Wait for tester to approve manual verification') {
            when { branch 'master' }
            steps {
                input message: "Approve manual verification?", ok: "Yes"
            }
        }
        stage('Deploy to production') {
            when { branch 'master' }
            agent any
            steps {
                script {
                    if (env.jobAborted == 'true') {
                        error('Job was aborted')
                    }
                    version = versionFromCommitMessage()
                    currentBuild.description = "Deploying ${version} to production environment"
                    sh "ssh ${productionDeployHostUser}@${productionDeployHostName} bash -s -- < pipelinex/application.sh update ${version}"
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

String versionFromCommitMessage() {
    return readCommitMessage().tokenize(':')[0]
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

static def issueId(def branchName) {
    return branchName.tokenize('/')[-1]
}

String issueStatus(def issueId) {
    return jiraGetIssue(idOrKey: issueId, site: 'jira').data.fields['status']['id']
}

def readCommitId() {
    return sh(returnStdout: true, script: 'git rev-parse HEAD').trim().take(7)
}

def readCommitMessage() {
    return sh(returnStdout: true, script: 'git log -1 --pretty=%B').trim()
}
