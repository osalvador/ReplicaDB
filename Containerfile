FROM registry.access.redhat.com/ubi9/openjdk-11-runtime
LABEL maintainer Oscar Salvador Magallanes
LABEL maintainer Francesco Zanti <francesco@tekapp.it> 

ENV USERNAME="replicadb"

USER root 
RUN microdnf install wget tar gzip -y

RUN useradd -ms /bin/bash ${USERNAME} && usermod -aG ${USERNAME} ${USERNAME}

USER "${USERNAME}:${USERNAME}"

ARG REPLICADB_RELEASE_VERSION=0.0.0
ENV REPLICADB_VERSION=$REPLICADB_RELEASE_VERSION

WORKDIR /home/${USERNAME}

COPY "ReplicaDB-${REPLICADB_VERSION}.tar.gz" /home/${USERNAME}

RUN tar -xvzf ReplicaDB-${REPLICADB_VERSION}.tar.gz
RUN rm ReplicaDB-${REPLICADB_VERSION}.tar.gz

ENV JAVA_HOME /usr
RUN export JAVA_HOME

ENTRYPOINT ["sh", "/home/replicadb/bin/replicadb","--options-file","/home/replicadb/conf/replicadb.conf" ]
