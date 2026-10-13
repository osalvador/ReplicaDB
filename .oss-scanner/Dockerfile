FROM eclipse-temurin:17

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      maven git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Network is available only during this build; the scanner runs offline afterwards.
COPY . /src
WORKDIR /src

# org.replicadb:ReplicaDB:1.0.2 is not on Maven Central, so install it locally first.
RUN mvn -B -ntp install -DskipTests \
    && mvn -B -ntp -f replicadb-server/pom.xml dependency:go-offline

# The frontend plugin downloads Node and npm; the surefire provider must be cached here because the scanner runs offline.
RUN mvn -B -ntp -f replicadb-server/pom.xml package -DskipTests \
    && mvn -B -ntp -f replicadb-server/pom.xml dependency:get -Dartifact=org.apache.maven.surefire:surefire-junit-platform:3.2.5 \
    && mvn -B -ntp -f replicadb-server/pom.xml dependency:get -Dartifact=org.junit.platform:junit-platform-launcher:6.0.3
