#!/usr/bin/env bash

die() {
    ret=$?
    message=${1-"[Failed (${ret})]"}
    echo ${message}
    exit ${ret}
}

warn() {
    message=$1
    echo ${message}
}

ok() {
    echo "[OK]"
}

requireArgument() {
    test -z "${!1}" && die "Missing argument '${1}'"
}

start() {
    id=${1}
    message=${2}
    requireArgument 'id'
    requireArgument 'message'
    [ "$(currentBranch)" == "$(masterBranch)" ] || die "Feature must be created from branch \"$(masterBranch)\""
    fetchMasterBranch
    [ -z "$(git diff --summary FETCH_HEAD)" ] || die "Local branch and remote diverges"
    featureBranch="feature/${id}"
    git checkout -b ${featureBranch}
    editChangesLog "${message}"
}

editChangesLog() {
    message=${1}
    requireArgument 'message'
    logEntry="$(currentBranch): ${message}" # TODO: Enforce max 50 char message
    tmpDir=$(mktemp -d "${TMPDIR:-/tmp/}XXXXXXXXXXXX")
    tmpFile="${tmpDir}/Changes.txt"
    echo "${logEntry}" | cat - $(changesFile) > ${tmpFile} && mv ${tmpFile} $(changesFile)
    rm -r ${tmpDir}
    git add $(changesFile)
    git commit -m "${logEntry}"
}

prepareQA() {
    echo -n "Verifying that current branch is a feature branch: "
    [[ $(currentBranch) =~ feature/.* ]] && ok || die
    echo -n "Verifying that current branch is integratable: "
    isIntegratable && ok || die
    echo -n "Creating QA branch \"$(qaBranch)\": "
    createQABranch && ok || die
    echo -n "Publishing QA branch... "
    publishBranch && ok || die
}

integrate() {
    echo "Integrating feature..."
    echo -n "Verifying that current branch is feature branch... "
    currentBranch=$(currentBranch)
    [[ ${currentBranch} =~ feature/.* ]] && ok || die "Not a feature branch: ${currentBranch}"
    echo -n "Verifying that current branch is integratable: "
    isIntegratable && ok || die
    featureBranch=${currentBranch}
    echo -n "Checking out $(masterBranch)... "
    git checkout $(masterBranch) && ok || die
    echo -n "Merging into $(masterBranch)... "
    git merge --ff-only --squash ${featureBranch} && ok || die
    echo -n "Committing... "
    git commit -m "$(head -1 $(changesFile))"
    echo -n "Pushing $(masterBranch) with feature..."
    git push && ok || die
    echo -n "Deleting feature branch on remote... "
    git push origin --delete ${featureBranch} && ok || die
    echo -n "Deleting feature branch locally... "
    git branch -D ${featureBranch}
}

isIntegratable() {
    git branch --contains $(masterBranch) | grep $(currentBranch) > /dev/null
}

createQABranch() {
    git checkout -q -b $(qaBranch) > /dev/null
}

fetchMasterBranch() {
    git fetch -q origin $(masterBranch) > /dev/null
}

currentBranch() {
    git symbolic-ref --short -q HEAD
}

qaBranch() {
    id=$(echo $(currentBranch) | cut -d'/' -f2)
    echo "feature/qa/${id}"
}

masterBranch() {
    echo 'master'
}

changesFile() {
    echo 'doc/Changes.txt'
}

publishBranch() {
    git push -u origin $(currentBranch)
}

case $1 in *)
        function=$1
        shift
        ${function} "$@"
        ;;
esac
