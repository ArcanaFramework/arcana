FROM tomcat:9-jre8-alpine

# User configurable arguments
ARG XNAT_VER=1.8.3
ARG XNAT_CS_PLUGIN_VER=3.1.0
ARG XNAT_ROOT=/data/xnat
ARG XNAT_HOME=/data/xnat/home
ARG JAVA_MS=256m
ARG JAVA_MX=2g
ARG XNAT_DATASOURCE_DRIVER=org.postgresql.Driver
ARG XNAT_DATASOURCE_URL=jdbc:postgresql://localhost:5432/xnat
ARG XNAT_DATASOURCE_USERNAME=xnat
ARG XNAT_DATASOURCE_PASSWORD=xnat
ARG XNAT_HIBERNATE_DIALECT=org.hibernate.dialect.PostgreSQL9Dialect
ARG TOMCAT_XNAT_FOLDER=ROOT
ARG TOMCAT_XNAT_FOLDER_PATH=${CATALINA_HOME}/webapps/${TOMCAT_XNAT_FOLDER}

# Create XNAT config file at $XNAT_HOME/config/xnat-conf.properties
COPY make-xnat-config.sh /usr/local/bin/make-xnat-config.sh

# Install XNAT and postgres
RUN apk add --no-cache postgresql postgresql-client supervisor wget
RUN rm -rf ${CATALINA_HOME}/webapps/*
RUN mkdir -p \
        ${TOMCAT_XNAT_FOLDER_PATH} \
        ${XNAT_HOME}/config \
        ${XNAT_HOME}/logs \
        ${XNAT_HOME}/plugins \
        ${XNAT_HOME}/work \
        ${XNAT_ROOT}/archive \
        ${XNAT_ROOT}/build \
        ${XNAT_ROOT}/cache \
        ${XNAT_ROOT}/ftp \
        ${XNAT_ROOT}/pipeline \
        ${XNAT_ROOT}/prearchive
RUN /usr/local/bin/make-xnat-config.sh
RUN rm /usr/local/bin/make-xnat-config.sh
RUN wget --no-verbose --output-document=/tmp/xnat-web-${XNAT_VER}.war https://api.bitbucket.org/2.0/repositories/xnatdev/xnat-web/downloads/xnat-web-${XNAT_VER}.war
RUN unzip -o -d ${TOMCAT_XNAT_FOLDER_PATH} /tmp/xnat-web-${XNAT_VER}.war
RUN rm -f /tmp/xnat-web-${XNAT_VER}.war
RUN wget --output-document ${XNAT_HOME}/plugins/container-service-plugin.jar https://bitbucket.org/xnatdev/container-service/downloads/container-service-${XNAT_CS_PLUGIN_VER}-fat.jar
RUN apk del wget
RUN mkdir /var/log/tomcat
ENV XNAT_HOME=${XNAT_HOME} XNAT_DATASOURCE_USERNAME=${XNAT_DATASOURCE_USERNAME} PGPASSWORD=${XNAT_DATASOURCE_PASSWORD}
ENV CATALINA_OPTS="-Xms${JAVA_MS} -Xmx${JAVA_MX} -Dxnat.home=${XNAT_HOME}"

# Add scripts for configuring XNAT database before launching Tomcat
COPY launch-xnat.sh /launch-xnat.sh
COPY XNAT.sql /XNAT.sql

# Skip the new site setup by creating site preferences file
COPY xnat-prefs-init.ini ${XNAT_HOME}/config/prefs-init.ini

# Expose the default Tomcat port
EXPOSE 8080

# Setup Postgres
RUN mkdir /run/postgresql
RUN mkdir /var/lib/postgresql/data
RUN chmod 700 /var/lib/postgresql/data
RUN chown postgres:postgres /run/postgresql /var/lib/postgresql/data

# Create postgres DB
USER postgres
RUN initdb -D /var/lib/postgresql/data
RUN echo -e "\ndatasource.driver = '/tmp'" >> /var/lib/postgresql/data/postgresql.conf
USER root


# Install Docker using recipe from Official Docker Dockerfile: https://github.com/docker-library/docker/blob/8b8d62e7eb791b060cc75cb2956724a1bdc5484b/20.10/Dockerfile
RUN apk add --no-cache \
		ca-certificates \
# Workaround for golang not producing a static ctr binary on Go 1.15 and up https://github.com/containerd/containerd/issues/5824
		libc6-compat \
# DOCKER_HOST=ssh://... -- https://github.com/docker/cli/pull/1014
		openssh-client

# set up nsswitch.conf for Go's "netgo" implementation (which Docker explicitly uses)
# - https://github.com/docker/docker-ce/blob/v17.09.0-ce/components/engine/hack/make.sh#L149
# - https://github.com/golang/go/blob/go1.9.1/src/net/conf.go#L194-L275
# - docker run --rm debian:stretch grep '^hosts:' /etc/nsswitch.conf
RUN [ ! -e /etc/nsswitch.conf ] && echo 'hosts: files dns' > /etc/nsswitch.conf

ENV DOCKER_VERSION 20.10.9
# TODO ENV DOCKER_SHA256
# https://github.com/docker/docker-ce/blob/5b073ee2cf564edee5adca05eee574142f7627bb/components/packaging/static/hash_files !!
# (no SHA file artifacts on download.docker.com yet as of 2017-06-07 though)


# Copied from Official Docker-in-Docker Dockerfile: https://github.com/docker-library/docker/blob/8baa881aab85f8398d2edbbcc0da4bd1f556dd98/20.10/dind/Dockerfile
RUN set -eux; \
	\
	apkArch="$(apk --print-arch)"; \
	case "$apkArch" in \
		'x86_64') \
			url='https://download.docker.com/linux/static/stable/x86_64/docker-20.10.9.tgz'; \
			;; \
		'armhf') \
			url='https://download.docker.com/linux/static/stable/armel/docker-20.10.9.tgz'; \
			;; \
		'armv7') \
			url='https://download.docker.com/linux/static/stable/armhf/docker-20.10.9.tgz'; \
			;; \
		'aarch64') \
			url='https://download.docker.com/linux/static/stable/aarch64/docker-20.10.9.tgz'; \
			;; \
		*) echo >&2 "error: unsupported architecture ($apkArch)"; exit 1 ;; \
	esac; \
	\
	wget -O docker.tgz "$url"; \
	\
	tar --extract \
		--file docker.tgz \
		--strip-components 1 \
		--directory /usr/local/bin/ \
	; \
	rm docker.tgz; \
	\
	dockerd --version; \
	docker --version

# COPY modprobe.sh /usr/local/bin/modprobe
# COPY docker-entrypoint.sh /usr/local/bin/

# https://github.com/docker-library/docker/pull/166
#   dockerd-entrypoint.sh uses DOCKER_TLS_CERTDIR for auto-generating TLS certificates
#   docker-entrypoint.sh uses DOCKER_TLS_CERTDIR for auto-setting DOCKER_TLS_VERIFY and DOCKER_CERT_PATH
# (For this to work, at least the "client" subdirectory of this path needs to be shared between the client and server containers via a volume, "docker cp", or other means of data sharing.)
ENV DOCKER_TLS_CERTDIR=/certs
# also, ensure the directory pre-exists and has wide enough permissions for "dockerd-entrypoint.sh" to create subdirectories, even when run in "rootless" mode
RUN mkdir /certs /certs/client && chmod 1777 /certs /certs/client
# (doing both /certs and /certs/client so that if Docker does a "copy-up" into a volume defined on /certs/client, it will "do the right thing" by default in a wa

# https://github.com/docker/docker/blob/master/project/PACKAGERS.md#runtime-dependencies
RUN set -eux; \
	apk add --no-cache \
		btrfs-progs \
		e2fsprogs \
		e2fsprogs-extra \
		ip6tables \
		iptables \
		openssl \
		shadow-uidmap \
		xfsprogs \
		xz \
# pigz: https://github.com/moby/moby/pull/35697 (faster gzip implementation)
		pigz \
	; \
# only install zfs if it's available for the current architecture
# https://git.alpinelinux.org/cgit/aports/tree/main/zfs/APKBUILD?h=3.6-stable#n9 ("all !armhf !ppc64le" as of 2017-11-01)
# "apk info XYZ" exits with a zero exit code but no output when the package exists but not for this arch
	if zfs="$(apk info --no-cache --quiet zfs)" && [ -n "$zfs" ]; then \
		apk add --no-cache zfs; \
	fi

# TODO aufs-tools

# set up subuid/subgid so that "--userns-remap=default" works out-of-the-box
RUN set -eux; \
	addgroup -S dockremap; \
	adduser -S -G dockremap dockremap; \
	echo 'dockremap:165536:65536' >> /etc/subuid; \
	echo 'dockremap:165536:65536' >> /etc/subgid

# https://github.com/docker/docker/tree/master/hack/dind
ENV DIND_COMMIT 42b1175eda071c0e9121e1d64345928384a93df1

RUN set -eux; \
	wget -O /usr/local/bin/dind "https://raw.githubusercontent.com/docker/docker/${DIND_COMMIT}/hack/dind"; \
	chmod +x /usr/local/bin/dind

COPY dockerd-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/dockerd-entrypoint.sh

VOLUME /var/lib/docker
EXPOSE 2375 2376
RUN mkdir /var/log/docker


# Use supervisord to launch postgres and tomcat
COPY supervisord.conf /etc/supervisord.conf
CMD ["/usr/bin/supervisord"]

