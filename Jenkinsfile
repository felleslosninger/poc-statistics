node {
    checkout scm
    if (env.BRANCH_NAME == 'develop') {
        sh 'mvn clean deploy'
    } else {
        sh 'mvn clean verify'
    }
}
