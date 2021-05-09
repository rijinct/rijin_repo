#
#       Health Monitoring Docker File
#

# Getting python and alpine 3.7 image from Repo
FROM registry1-docker-io.repo.lab.pl.alcatel-lucent.com/python:3.7-alpine3.8

# Getting Open Jdk 8 from alpine repo
ARG alpine_repo_fqdn=repo.lab.pl.alcatel-lucent.com
ARG alpine_revision=v3.7

RUN echo "http://${alpine_repo_fqdn}/alpine/${alpine_revision}/main" >/etc/apk/repositories && \
    echo "http://${alpine_repo_fqdn}/alpine/${alpine_revision}/community" >>/etc/apk/repositories && \
    echo '[global]' > /etc/pip.conf && \
    echo "trusted-host = ${alpine_repo_fqdn}" >> /etc/pip.conf && \
    echo "index-url = https://${alpine_repo_fqdn}/api/pypi/python/simple/" >> /etc/pip.conf && \
    echo "index = https://${alpine_repo_fqdn}/api/pypi/python/simple/" >> /etc/pip.conf && \
    echo 'disable-pip-version-check = True' >> /etc/pip.conf
	
RUN apk add \
        python3-dev \
        postgresql-libs \
        gcc \
        musl-dev \
        postgresql-dev \
        shadow \
        sudo \
        openjdk8-jre \
        libstdc++ \
        g++ \
        libxml2-dev \
        libxslt-dev

RUN pip install kafka-python==1.4.4
RUN pip install python-dateutil==2.7.5
RUN pip install requests
RUN pip install numpy==1.19.4 --proxy=10.158.100.6:8080
RUN pip install pandas3 --no-binary :all --proxy=10.158.100.6:8080
RUN pip install lxml --proxy=10.158.100.6:8080
RUN pip install dicttoxml --proxy=10.158.100.6:8080
RUN pip install flask --proxy=10.158.100.6:8080
RUN pip install flask_restful --proxy=10.158.100.6:8080
RUN pip install flasgger --proxy=10.158.100.6:8080
RUN pip install kubernetes --proxy=10.158.100.6:8080

#Setting Cron
RUN sed -e 's/# %wheel ALL=(ALL) NOPASSWD: ALL/%wheel ALL=(ALL) NOPASSWD: ALL/g' \
                -i /etc/sudoers
RUN echo "ngdb ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN mkdir -p /ngdb/
ADD conf/crontab.txt /ngdb/crontab.txt
COPY scripts/entrypoint.sh /ngdb/entrypoint.sh
COPY scripts/update-passwd-file.sh /ngdb/
RUN chmod 777 /ngdb


# Setting Environment Variables
ENV NGDB_HOME /opt/nsn/ngdb

# Creating needed directory structure
WORKDIR $NGDB_HOME/monitoring/
RUN mkdir -p scripts utils python-dependencies ext-dependencies extEndPointsConfigMap output resources tools /usr/local/lib/python3.7/site-packages/unified_logging cad

# Installing the Python 3rd Party Packages
RUN pip3 install py4j==0.10.8.1 --proxy=10.158.100.6:8080
WORKDIR $NGDB_HOME/monitoring/

# Copy of Framework & Installation
COPY framework/* .
RUN pip3 install healthmonitoring-framework-0.1.tar.gz --proxy=10.158.100.6:8080
COPY resources/ ./resources/
COPY unified_logging/* /usr/local/lib/python3.7/site-packages/unified_logging/
COPY logger.py /usr/local/lib/python3.7/site-packages/
COPY tools/ ./tools/

# Copy of Collectors & Installation
COPY collectors/* .
RUN pip3 install health-monitoring-collectors-0.1.tar.gz --proxy=10.158.100.6:8080


# Copying of utils, scripts, conf and required libs to required directories
COPY utils/* $NGDB_HOME/monitoring/utils/
COPY scripts/* $NGDB_HOME/monitoring/scripts/
COPY cad $NGDB_HOME/monitoring/cad/
COPY conf/crontab.txt $NGDB_HOME/monitoring/conf/
COPY hive-lib/* $NGDB_HOME/monitoring/ext-dependencies/hive-lib/
COPY spark-lib/* $NGDB_HOME/monitoring/ext-dependencies/spark-lib/

RUN ln -sf /monitoring/conf/monitoring.xml $NGDB_HOME/monitoring/conf/monitoring.xml

RUN chmod a+w /etc/passwd /etc/group

#Entry Point for Pod 	
ENTRYPOINT [ "sh", "/ngdb/entrypoint.sh"]
