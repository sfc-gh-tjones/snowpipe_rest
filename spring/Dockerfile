FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /work
COPY src/ src
COPY pom.xml .
COPY mvnw .
RUN mvn clean package

FROM eclipse-temurin:21
EXPOSE 8080
WORKDIR /app
COPY --from=build /work/target/*.jar .
RUN groupadd -r appuser && \
    useradd -r -g appuser appuser && \
    chown -R appuser:appuser /app
USER appuser
ENTRYPOINT [ "java", "-Xms56g", "-Xmx56g", "-XX:+HeapDumpOnOutOfMemoryError", "-jar", "SnowpipeRest-0.0.1-SNAPSHOT.jar"]
