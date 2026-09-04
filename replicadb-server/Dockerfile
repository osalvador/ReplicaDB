FROM eclipse-temurin:17-jre-noble

ARG SERVER_VERSION=0.19.0
ARG SERVER_JAR=target/replicadb-server-${SERVER_VERSION}.jar

ENV SPRING_PROFILES_ACTIVE=api
ENV JAVA_OPTS=

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system replicadb \
    && useradd --system --gid replicadb --create-home replicadb \
    && mkdir -p /opt/replicadb \
    && chown -R replicadb:replicadb /opt/replicadb

WORKDIR /opt/replicadb
COPY --chown=replicadb:replicadb ${SERVER_JAR} /opt/replicadb/replicadb-server.jar

USER replicadb:replicadb

EXPOSE 8080 9091

ENTRYPOINT ["sh", "-c", "exec java ${JAVA_OPTS:-} -Dreplicadb.embedded-postgres.enabled=false -Dspring.profiles.active=${SPRING_PROFILES_ACTIVE:-api} -jar /opt/replicadb/replicadb-server.jar"]
