FROM openjdk:8-jre-alpine

RUN apk add --no-cache bash

ARG REPLICADB_RELEASE_VERSION=0.0.0
ENV REPLICADB_VERSION=$REPLICADB_RELEASE_VERSION
ENV USERNAME="replicadb"

RUN addgroup -S ${USERNAME} && adduser -S ${USERNAME} -G ${USERNAME}
USER "${USERNAME}:${USERNAME}"

WORKDIR /home/${USERNAME}


COPY "ReplicaDB-${REPLICADB_VERSION}.tar.gz" /home/${USERNAME}

RUN tar -xvzf ReplicaDB-${REPLICADB_VERSION}.tar.gz
RUN rm ReplicaDB-${REPLICADB_VERSION}.tar.gz


ENTRYPOINT ["bash", "/home/replicadb/bin/replicadb","--options-file","/home/replicadb/conf/replicadb.conf" ]
