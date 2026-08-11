FROM eclipse-temurin:17-jre-noble

RUN apt-get update \
	&& apt-get install -y --no-install-recommends bash \
	&& rm -rf /var/lib/apt/lists/*

ARG REPLICADB_RELEASE_VERSION=0.0.0
ENV REPLICADB_VERSION=$REPLICADB_RELEASE_VERSION
ENV USERNAME="replicadb"

RUN groupadd --system ${USERNAME} && useradd --system --gid ${USERNAME} --create-home ${USERNAME}
USER "${USERNAME}:${USERNAME}"

WORKDIR /home/${USERNAME}


COPY "ReplicaDB-${REPLICADB_VERSION}.tar.gz" /home/${USERNAME}

RUN tar -xvzf ReplicaDB-${REPLICADB_VERSION}.tar.gz
RUN rm ReplicaDB-${REPLICADB_VERSION}.tar.gz


ENTRYPOINT ["bash", "/home/replicadb/bin/replicadb","--options-file","/home/replicadb/conf/replicadb.conf" ]
