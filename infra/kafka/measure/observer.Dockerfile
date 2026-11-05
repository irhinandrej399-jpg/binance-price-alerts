FROM eclipse-temurin:21-jdk-alpine@sha256:fd52084c35bbac78890d95d57707d31ffc9d7a3324199e89c06be2a9dc71bfa0
RUN apk add --no-cache strace=6.19-r1
ENTRYPOINT ["strace"]
