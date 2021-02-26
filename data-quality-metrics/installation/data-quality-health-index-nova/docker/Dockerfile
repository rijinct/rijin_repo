#
#       DQHI Docker File
#

FROM csf-docker-delivered.repo.lab.pl.alcatel-lucent.com/k8s/cspk-python:2.7.1-2.4.5-3110

ENV NGDB_HOME /opt/nsn/ngdb
ENV SCHEDULER_CONF_LOCATION /opt/nsn/ngdb/scheduler/app/conf/
ENV HADOOP_CONF_DIR /etc/hadoop/conf

RUN sed -i 's/\/usr\/bin\/python/\/usr\/bin\/python2/g' /usr/bin/yum
RUN sed -i 's/\/usr\/bin\/python/\/usr\/bin\/python2/g' /usr/libexec/urlgrabber-ext-down

RUN yum -y install yum-plugin-ovl
RUN yum -y install krb5-workstation
RUN yum -y install postgresql-libs \
                   gcc \
                   musl-dev \
                   postgresql-dev \
                   shadow \
                   sudo	\
                   cronie				

RUN pip3 install PyYAML --proxy=10.158.100.6:8080
RUN pip3 install psycopg2-binary --proxy=10.158.100.6:8080
RUN pip3 install sqlparse --proxy=10.158.100.6:8080
RUN pip3 install flask --proxy=10.158.100.6:8080
RUN pip3 install flask_restful --proxy=10.158.100.6:8080
RUN pip3 install py4j==0.10.8.1 --proxy=10.158.100.6:8080
RUN pip3 install flasgger --proxy=10.158.100.6:8080
RUN pip3 install sqlalchemy --proxy=10.158.100.6:8080
RUN pip3 install xlrd==1.2.0 --proxy=10.158.100.6:8080

#Setting Cron 
RUN sed -i 's/^%wheel\s*\(ALL=(ALL)\s\+ALL\)/# %wheel \1/' /etc/sudoers
RUN sed -i 's/^#\s*\(%wheel\s\+ALL=(ALL)\s\+NOPASSWD:\s\+ALL\)/\1/' /etc/sudoers

RUN echo "ngdb ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
RUN mkdir -p /ngdb/
#ADD conf/crontab.txt /ngdb/crontab.txt
COPY scripts/entrypoint.sh /ngdb/entrypoint.sh
COPY scripts/update-passwd-file.sh /ngdb/
COPY conf/ /ngdb/conf
RUN chmod -R 777 /ngdb

# Setting Environment Variables
ENV DQHI_HOME /opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/

# Creating needed directory structure
WORKDIR $DQHI_HOME
RUN mkdir -p com/rijin/dqhi output sql test conf/custom /usr/local/lib/python3.6/site-packages/unified_logging

# Copy of Framework & Installation
COPY scripts $DQHI_HOME/com/rijin/dqhi
COPY sql/ $DQHI_HOME/sql/
COPY test/ $DQHI_HOME/test/
COPY lib/* $DQHI_HOME/ext-dependencies/
COPY unified_logging/* /usr/local/lib/python3.6/site-packages/unified_logging/
COPY logger.py /usr/local/lib/python3.6/site-packages/

#support for spark on yarn
RUN rm /opt/spark/jars/hadoop*
RUN rm /opt/spark/jars/parquet*
RUN rm /opt/spark/jars/spark*
RUN rm /opt/spark/jars/commons-configuration-1.6.jar
RUN rm /opt/spark/jars/htrace-core*
RUN rm /opt/spark/jars/hive*
RUN rm /opt/spark/jars/datanucleus-*
RUN rm /opt/spark/jars/log4j-1.2-api-*
RUN rm /opt/spark/jars/log4j-slf4j-impl-*
COPY pyspark-lib/* /opt/spark/jars/


RUN chmod a+w /etc/passwd /etc/group
RUN chmod -R 775 /opt/spark/
RUN chmod -R 775 /opt/cspk/
RUN chmod -R 777 $DQHI_HOME
RUN chmod 775 /usr/local/lib/python3.6/site-packages/logger.py
RUN chmod -R 775 /usr/local/lib/python3.6/site-packages/unified_logging
RUN rm -f /etc/localtime

ENTRYPOINT [ "sh", "/ngdb/entrypoint.sh" ]

EXPOSE 5001

