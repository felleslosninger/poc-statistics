FROM docker:17.06.2

RUN apk add --no-cache coreutils git openssh-client curl zip unzip bash ttf-dejavu ca-certificates openssl groff py-pip python jq
RUN pip install awscli && apk --purge -v del py-pip
