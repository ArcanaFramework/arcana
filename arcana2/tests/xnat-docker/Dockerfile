FROM tomcat:7-jre8-alpine

ARG XNAT_VER=1.8.2
ARG XNAT_ROOT=/data/xnat
ARG XNAT_HOME=/data/xnat/home
ARG XNAT_DATASOURCE_DRIVER=org.postgresql.Driver
ARG XNAT_DATASOURCE_URL=jdbc:postgresql://localhost:5432/xnat
ARG XNAT_DATASOURCE_USERNAME=xnat
ARG XNAT_DATASOURCE_PASSWORD=xnat
ARG XNAT_HIBERNATE_DIALECT=org.hibernate.dialect.PostgreSQL9Dialect
ARG TOMCAT_XNAT_FOLDER=ROOT
ARG TOMCAT_XNAT_FOLDER_PATH=${CATALINA_HOME}/webapps/${TOMCAT_XNAT_FOLDER}

COPY make-xnat-config.sh /usr/local/bin/make-xnat-config.sh

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
RUN apk del wget

RUN mkdir /var/log/tomcat

EXPOSE 8080

ENV XNAT_HOME=${XNAT_HOME} XNAT_DATASOURCE_USERNAME=${XNAT_DATASOURCE_USERNAME} PGPASSWORD=${XNAT_DATASOURCE_PASSWORD}

# Setup postgres
RUN mkdir /run/postgresql
RUN mkdir /var/lib/postgresql/data
RUN chmod 700 /var/lib/postgresql/data
RUN chown postgres:postgres /run/postgresql /var/lib/postgresql/data
COPY XNAT.sql /XNAT.sql
COPY launch-xnat.sh /launch-xnat.sh

USER postgres
RUN initdb -D /var/lib/postgresql/data
RUN echo -e "\ndatasource.driver = '/tmp'" >> /var/lib/postgresql/data/postgresql.conf
USER root

# Set up configuration files
COPY supervisord.conf /etc/supervisord.conf

CMD ["/usr/bin/supervisord"]

