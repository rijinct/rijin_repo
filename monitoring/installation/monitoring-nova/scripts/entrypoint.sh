
sh /ngdb/update-passwd-file.sh

sudo mkdir -p /var/local/monitoring/ /opt/nsn/ngdb/monitoring/output/log

sudo chown -R ngdb:ninstall /opt/nsn/ngdb/monitoring/resources /opt/nsn/ngdb/monitoring/output/

sudo ln -s /opt/nsn/ngdb/monitoring/output/log /var/local/monitoring

# Generating Environment Variables
printenv  | grep -v "CRONTAB_PROPERTIES" | grep -v "^\"" | grep -v "^}" | grep -v "^," | sed 's/^\(.*\)$/export \1/g' | sed 's/=/="/' | sed 's/$/"/g' > /ngdb/monitoringEnv.sh

# Creating the table
python /opt/nsn/ngdb/monitoring/utils/createTable.py

#Populating Hosts in Hosts.yaml
python /opt/nsn/ngdb/monitoring/tools/populate_hosts_k8s.py /opt/nsn/ngdb/monitoring/resources/hosts.yaml /opt/nsn/ngdb/monitoring/resources/hosts.yaml

# Generating Audit events
python /opt/nsn/ngdb/monitoring/tools/audit_generator.py /opt/nsn/ngdb/monitoring/resources/

# Running framework in background
python -m healthmonitoring.framework /opt/nsn/ngdb/monitoring/resources/monitoring.properties &

# Running cad/monitoring controller in background
nohup python /opt/nsn/ngdb/monitoring/cad/scripts/rest/monitoring_controller.py | tee /opt/nsn/ngdb/monitoring/output/controller.log 2>&1 & 

# Addition of Cron entries for the user ngdb
sudo /usr/bin/crontab -u ngdb /ngdb/crontab.txt

# start cron
sudo touch /var/log/cron.log
sudo /usr/sbin/crond -f -l 0 -L /var/log/cron.log