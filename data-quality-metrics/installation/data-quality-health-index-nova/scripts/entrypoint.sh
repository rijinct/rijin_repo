sh /ngdb/update-passwd-file.sh
#sudo chown -R ngdb:ninstall /opt/nsn/ngdb/data-quality-metrics


# Creating DQHI tables
python3 /opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/create_table.py CREATE

# Copy directories to pvc mnt
sh /opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/copy_files_to_mnt.sh

# Restart DQHI in case of pod restart
python3 -m com.rijin.dqhi.score_calculator_wrapper_nova &

# start REST services
python3 /opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/com/rijin/dqhi/rest/DQHIController.py

