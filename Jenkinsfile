node {
    checkout scm
    if (env.BRANCH_NAME == 'develop') {
    	sh 'mvn clean versions:set deploy'
    } else {
        sh 'mvn clean verify'
    }
}
