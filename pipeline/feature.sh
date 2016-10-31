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
    test -z ${!1} && die "Missing argument '${1}'"
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
    commitMessage="$(currentBranch): ${message}" # TODO: Enforce max 50 char message
    tmpFile="/tmp/${id}_Changes.md"
    changesFile="doc/Changes.md"
    echo "${commitMessage}" | cat - ${changesFile} > ${tmpFile} && mv ${tmpFile} ${changesFile}
    git add ${changesFile}
    git commit -m "${commitMessage}"
}

prepareQA() {
    [[ $(currentBranch) =~ feature/.* ]] && ok || die "Must be on feature branch"
    git checkout -b $(qaBranch)
    git rebase origin/$(masterBranch) || die
    publishBranch
}

integrate() {
    echo "Integrating feature..."
    echo -n "Verifying that current branch is QA branch... "
    currentBranch=$(currentBranch)
    [[ ${currentBranch} =~ feature/qa/.* ]] && ok || die "Not a QA branch: ${currentBranch}"
    echo -n "Rebasing onto $(masterBranch)... "
    git rebase -i --autosquash $(masterBranch) && ok || die
    qaBranch=${currentBranch}
    echo -n "Checking out $(masterBranch)... "
    git checkout $(masterBranch) && ok || die
    echo -n "Merging into $(masterBranch)... "
    git merge --ff-only ${qaBranch} && ok || die
    echo -n "Pushing $(masterBranch) with feature..."
    git push && ok || die
    echo -n "Deleting QA branch on remote... "
    git push origin --delete ${qaBranch} && ok || die
    echo -n "Deleting QA branch locally... "
    git branch -D ${qaBranch}
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

publishBranch() {
    git push -u origin $(currentBranch)
}

case $1 in *)
        function=$1
        shift
        ${function} "$@"
        ;;
esac
