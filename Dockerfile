FROM openjdk:8-jre-alpine

RUN apk add --no-cache bash

ARG replicadb_version=0.8.1
ENV REPLICADB_VERSION=$replicadb_version
ENV USERNAME="replicadb"

RUN addgroup -S ${USERNAME} && adduser -S ${USERNAME} -G ${USERNAME}
USER "${USERNAME}:${USERNAME}"

WORKDIR /home/${USERNAME}

#ADD https://github.com/osalvador/ReplicaDB/releases/download/v${REPLICADB_VERSION}/ReplicaDB-${REPLICADB_VERSION}.tar.gz /home/${USERNAME}
COPY ReplicaDB-${REPLICADB_VERSION}.tar.gz /home/${USERNAME}

RUN tar -xvzf ReplicaDB-${REPLICADB_VERSION}.tar.gz
RUN rm ReplicaDB-${REPLICADB_VERSION}.tar.gz


ENTRYPOINT ["bash", "/home/replicadb/bin/replicadb","--options-file","/home/replicadb/conf/replicadb.conf" ]
