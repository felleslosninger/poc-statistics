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
    test -z "${!1}" && die "Missing argument '${1}' ${2}"
}

start() {
    type=${1}
    id=${2}
    message=${3}
    requireArgument 'type' '(feature or bugfix)'
    requireArgument 'id'
    requireArgument 'message'
    [ "$(currentBranch)" == "$(masterBranch)" ] || die "Work branch must be created from \"$(masterBranch)\""
    fetchMasterBranch
    [ -z "$(git diff --summary FETCH_HEAD)" ] || die "Local branch and remote diverges"
    workBranch="${type}/${id}"
    git checkout -b ${workBranch}
    editChangesLog "${message}"
}

info() {
    isWorkBranch || echo "$(currentBranch) is not a work branch"
    echo "Change log entry: $(change)"
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

integrate() {
    echo "Integrating branch..."
    isIntegratable
    workBranch=$(currentBranch)
    echo -n "Checking out branch $(masterBranch): "
    git checkout $(masterBranch) && ok || die "Failed to check out branch $(masterBranch)"
    echo -n "Updating branch $(masterBranch): "
    git pull && ok || die "Failed to update branch $(masterBranch)"
    echo -n "Merging into $(masterBranch)... "
    git merge --ff-only --squash ${workBranch} && ok || die
    echo -n "Committing... "
    git commit -m "$(change)"
    echo -n "Pushing $(masterBranch) with new code..."
    git push && ok || die
    echo -n "Deleting work branch on remote... "
    git push origin --delete ${workBranch} && ok || die
    echo -n "Deleting work branch locally... "
    git branch -D ${workBranch}
}

isIntegratable() {
    workBranch=$(currentBranch)
    echo -n "Verifying that branch ${workBranch} is a bugfix/feature branch: "
    isWorkBranch && ok || die "${workBranch} is not a bugfix/feature branch"
    echo -n "Verifying that branch ${workBranch} has no unstaged changes: "
    git diff-files --quiet -- && ok || die "Found unstaged changes."
    echo -n "Verifying that branch ${workBranch} has no staged changes: "
    git diff-index --quiet --cached HEAD && ok || die "Found staged changes."
    echo -n "Verifying that branch ${workBranch} is synchronized with remote branch: "
    isSynchronizedWithRemote && ok || die "Local branch and remote diverges"
    echo -n "Verifying that branch ${workBranch} contains origin/$(masterBranch): "
    git branch --contains origin/$(masterBranch) | grep ${workBranch} > /dev/null && ok || die "No. Please run 'git merge origin/$(masterBranch)'."
}

isSynchronizedWithRemote() {
    [[ -z $(git --no-pager diff origin/$(currentBranch)) ]]
}

isWorkBranch() {
    [[ $(currentBranch) =~ (bugfix|feature)/.+ ]]
}

fetchMasterBranch() {
    git fetch -q origin $(masterBranch) > /dev/null
}

currentBranch() {
    git symbolic-ref --short -q HEAD
}

masterBranch() {
    echo 'master'
}

changesFile() {
    echo 'doc/Changes.txt'
}

change() {
    head -1 $(changesFile)
}

publish() {
    git push -u origin $(currentBranch)
}

case $1 in *)
        function=$1
        shift
        ${function} "$@"
        ;;
esac
