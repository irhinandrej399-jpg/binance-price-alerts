FROM apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837 AS kafka
FROM eclipse-temurin:21-jdk-alpine@sha256:fd52084c35bbac78890d95d57707d31ffc9d7a3324199e89c06be2a9dc71bfa0
COPY --from=kafka /opt/kafka/libs /opt/kafka/libs
WORKDIR /check
COPY VerifyKafka.java .
RUN javac -cp '/opt/kafka/libs/*' VerifyKafka.java
ENTRYPOINT ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-cp", "/opt/kafka/libs/*:/check", "VerifyKafka"]
