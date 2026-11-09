# Two stages: the first has a JDK and Maven and builds the jar; only the second, with just a
# Java runtime and the jar, becomes the image. Builds from a clean clone with no local Java.

FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
# Dependencies first, so they're cached and only re-downloaded when pom.xml changes.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src src
# Tests run in `./mvnw verify`; the image build only packages.
RUN mvn -B -q package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --no-create-home relay
COPY --from=build /src/target/relay.jar /app/relay.jar
USER relay
EXPOSE 9000
# Exec form, so the JVM is PID 1 and receives SIGTERM from `docker stop`; the shutdown hook then runs.
ENTRYPOINT ["java", "-jar", "/app/relay.jar"]
