FROM docker:17.09.0-ce@sha256:f4e7ce476a48644336040944429a710998bcc87c4a76a929582afe8a2086a171

RUN apk add --no-cache coreutils git openssh-client curl zip unzip bash ttf-dejavu ca-certificates openssl groff py-pip python jq
RUN pip install awscli && apk --purge -v del py-pip
