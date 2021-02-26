#!/bin/bash
project_application_hosts=(smaster1.rijin.com smaster2.rijin.com sslave1.rijin.com sslave2.rijin.com sslave3.rijin.com analytics.rijin.com)
project_installation_type="cluster"

#####  scheduler_definition #####
project_scheduler_ha_status="yes"
project_scheduler_active_fip_host="scheduler-fip.rijin.com"
project_scheduler_hosts=(smaster1.rijin.com smaster2.rijin.com)
project_scheduler_ha_hosts=(smaster1.rijin.com smaster2.rijin.com)

#####  postgres_definition #####
project_postgres_sdk_ha_status="yes"
project_postgres_sdk_active_fip_host="postgressdk-fip.rijin.com"
project_postgres_sdk_fip_active_host="postgressdk-fip.rijin.com"
project_postgres_hosts=(smaster1.rijin.com smaster2.rijin.com analytics.rijin.com)
project_postgres_ha_hosts=(smaster1.rijin.com smaster2.rijin.com)

#####  keepalived_definition #####
project_keepalived_ha_hosts=(smaster1 smaster2)

#####  sdk_definition #####
project_sdk_db_enable_ssl="yes"
project_sdk_db_name="sai"
project_sdk_schema_name="sairepo"
project_application_sdk_dbtype="postgres"
project_application_sdk_db_port="5432"
project_application_sdk_db_driver="org.postgresql.Driver"
project_application_sdk_db_url="jdbc:postgresql://postgressdk-fip.rijin.com:5432/sai?ssl"
project_application_sdk_db_dialect="org.hibernate.dialect.PostgreSQLDialect"
project_application_sdk_database_linux_user="postgres"

#####  etl_definition #####
project_file_format="PARQUET"
project_etl_file_format="PARQUET"
rtb_input_data_type="csv"
project_med_data_in_utc="false"
num_of_etl_worker_nodes=""
project_etl_ha_status="no"
project_etl_type="kafka"
project_etl_active_fip_host="sslave1.rijin.com"
project_kafka_hosts=(smaster1.rijin.com smaster2.rijin.com sslave1.rijin.com sslave2.rijin.com sslave3.rijin.com)
project_kakfa_broker_hosts=(sslave1.rijin.com sslave2.rijin.com sslave3.rijin.com)
project_kafka_ui_hosts=(smaster1.rijin.com)
project_kafka_app_hosts=(sslave1.rijin.com sslave2.rijin.com sslave3.rijin.com)
project_etl_mntshare="/mntshare"
project_etl_new_app_node="false"
project_kafka_enabled="yes"

#####  cefk_definition #####
project_cefk_deployment_hosts=(smaster1.rijin.com)
project_cefk_deployment_hosts_public_ip=(10.53.74.126)
project_cefk_deployment_hosts_private_ip=(50.50.50.126)
project_kibana_hosts=(analytics.rijin.com)
project_kibana_hosts_public_ip=(10.53.74.130)
project_kibana_hosts_private_ip=(50.50.50.130)
project_elasticsearch_hosts=(analytics.rijin.com)
project_elasticsearch_hosts_public_ip=(10.53.74.130)
project_elasticsearch_hosts_private_ip=(50.50.50.130)
project_fluentd_hosts=(analytics.rijin.com)
project_kibana_port="5601"
project_elasticsearch_port="9200"

#####  couchbase_definition #####
project_couchbase_hosts=(smaster1.rijin.com smaster2.rijin.com sslave1.rijin.com)
project_couchbase_port="8091"
project_couchbase_admin_user_name="Administrator"
project_couchbase_admin_user_password="DB0AC656B6971C6CBE9E1ED807CFD8471E50C9E3412BB1F5"

#####  boxi_definition #####
project_boxi_hosts=(smaster1.rijin.com smaster2.rijin.com)
project_boxi_cms_port="6400"
project_boxi_admin_name="Administrator"
project_boxi_cms_pass="DB0AC656B6971C6CBE9E1ED807CFD8471E50C9E3412BB1F5"
project_boxi_enable="yes"

#####  grafana_definition #####
project_grafana_status="yes"
project_grafana_username="admin"
project_grafana_password="B731AE49544766A2C8A2B41375C15DF91DA88EA55EE80ABC"
project_grafana_port="3000"
project_grafana_ha_status="no"
project_grafana_active_fip_host="analytics.rijin.com"
project_grafana_fip="analytics.rijin.com"
project_grafana_hosts=(analytics.rijin.com)
project_grafana_hosts_public_ip=(10.53.74.130)
project_grafana_ha_hosts=()

#####  webservice_definition #####
project_webservice_ha_status="yes"
project_webservice_active_fip_host="webapp-fip.rijin.com"
project_webservice_hosts=(smaster1.rijin.com smaster2.rijin.com)
project_webservice_ha_hosts=(smaster1.rijin.com smaster2.rijin.com)

#####  dalserver_definition #####
project_dalserver_ha_status="yes"
project_dalserver_active_fip_host="dalserver-fip.rijin.com"
project_dalserver_hosts=(smaster1.rijin.com smaster2.rijin.com)
project_dalserver_ha_hosts=(smaster1.rijin.com smaster2.rijin.com)

#####  zookeeper_definition #####
project_kafka_zoo_hosts=(smaster1.rijin.com smaster2.rijin.com sslave1.rijin.com)

#####  analytics_definition #####
project_analytics_scheduler_client_hosts=(analytics.rijin.com)
project_analytics_r_hosts=(analytics.rijin.com)
project_analytics_h20_hosts=(analytics.rijin.com)
project_analytics_hosts=(analytics.rijin.com)
project_tomcat_hosts=(analytics.rijin.com)
project_application_analytics_db_url="jdbc:postgresql://analytics.rijin.com:5432/analytics-db?ssl"

#####  sdkrestservice_defination #####
project_sdkrestservice_status="no"
project_sdkrestservice_port="9601"
project_sdkrestservice_url=""
project_sdkrestservice_ha_status="no"
project_sdkrestservice_active_fip_host="smaster1.rijin.com"
project_sdkrestservice_fip=""
project_sdkrestservice_ha_hosts=()

#####  application_monitoring_definition #####
project_application_monitoring_hosts=(smaster1.rijin.com)

#####  portal_definition #####
project_portal_hosts=(portal1.rijin.com portal2.rijin.com)
project_portal_ha_status="yes"
project_portal_postgres_floating_ip="50.50.50.250"

#####  env_definition #####
project_application_installation_type="CEM"
project_hdfs_user="ngdb"
project_os_release_base_version="RHEL7"
project_os_release_base_version_rhel6="RHEL6"
project_os_release_base_version_rhel7="RHEL7"
project_version="VER-SP2"
project_hadoop_version="2.6.0"
project_hive_version="1.1.0"
project_spark_version="2.3.0"
project_hbase_version="1.2.0"
project_wshttps_version="YES"
project_start_day_of_the_week="monday"

#####  application_platform_interface_definition #####

#####  hdfs interface #####
project_spark_eventlog_dir="hdfs://projectcluster/user/spark/spark2ApplicationHistory"
project_historyserver_address="http://hmaster1.rijin.com:18089"
project_hdfs_zookeeper_hosts=()
project_hdfs_url=""
project_hdfs_master_hosts=(hmaster1.rijin.com hmaster2.rijin.com)
project_hdfs_connection_port=""

#####  hbase interface #####
project_hbase_zookeeper_hosts=()
project_hbase_url=""
project_hbase_hosts=(hmaster1.rijin.com hmaster2.rijin.com hutility1.rijin.com)
project_hbase_connection_port=""

#####  hive interface #####
project_hive_zookeeper_hosts=()
project_hive_url="jdbc:hive2://hive-fip.rijin.com:8071/project;principal=hive/50.50.50.200@RIJIN.COM;retries=3;ssl=true;sslTrustStore=/opt/cloudera/security/jks/jssecacerts;trustStorePassword=truststorepass"
project_hive_customisation_url="jdbc:hive2://hive-fip.rijin.com:8075/project;principal=hive/50.50.50.200@RIJIN.COM;retries=3;ssl=true;sslTrustStore=/opt/cloudera/security/jks/jssecacerts;trustStorePassword=truststorepass"
project_hive_hosts=(hmaster1.rijin.com hmaster2.rijin.com)
project_hive_customisation_hosts=(hutility1.rijin.com hutility2.rijin.com)
project_hive_schema="project"
project_hive_dbname="metastore"
project_hive_common_db="no"
project_hive_driver="org.apache.hive.jdbc.HiveDriver"
project_hive_export_jobs="/var/local/hive-export"
project_hive_connection_port=""

#####  spark interface #####
project_spark_zookeeper_hosts=()
project_spark_url=""
project_spark_hosts=(hmaster1.rijin.com)
project_spark_connection_port=""

#####  spark thrift interface #####
project_spark_thrift_zookeeper_hosts=()
project_spark_thrift_url="jdbc:hive2://spark-fip.rijin.com:8072/project;principal=spark/50.50.50.201@RIJIN.COM;retries=3;ssl=true;sslTrustStore=/opt/cloudera/security/jks/jssecacerts;trustStorePassword=truststorepass"
project_spark_thrift_hosts=(smaster1.rijin.com smaster2.rijin.com)
project_spark_thrift_connection_port=""

#####  spark thrift flexi interface #####
project_spark_thrift_flexi_zookeeper_hosts=()
project_spark_thrift_flexi_url="jdbc:hive2://spark-fip.rijin.com:8073/project;principal=spark/50.50.50.201@RIJIN.COM;retries=3;ssl=true;sslTrustStore=/opt/cloudera/security/jks/jssecacerts;trustStorePassword=truststorepass"
project_spark_thrift_flexi_hosts=(smaster1.rijin.com smaster2.rijin.com)
project_spark_thrift_flexi_connection_port=""

#####  spark thrift app interface #####
project_spark_thrift_app_zookeeper_hosts=()
project_spark_thrift_app_url="jdbc:hive2://spark-fip.rijin.com:8074/project;principal=spark/50.50.50.201@RIJIN.COM;retries=3;ssl=true;sslTrustStore=/opt/cloudera/security/jks/jssecacerts;trustStorePassword=truststorepass"
project_spark_thrift_app_hosts=(smaster1.rijin.com smaster2.rijin.com)
project_spark_thrift_app_connection_port=""

#####  platform_definition #####
project_platform_distribution_type="cloudera"
project_cloudera_manager="hutility1.rijin.com"

#####  application_user_definition #####
project_application_root_user="root"
project_application_root_user_group="root"
project_application_root_user_home_dir="/root"
project_application_ngdb_user="ngdb"
project_application_ngdb_user_group="ninstall"
project_application_ngdb_user_home_dir="/home/ngdb"
project_application_postgres_user="postgres"
project_application_postgres_user_group="ninstall"
project_application_postgres_user_home_dir="/home/postgres"
project_application_bobje_user="bobje"
project_application_bobje_user_group="ninstall"
project_application_bobje_user_home_dir="/home/bobje"
project_application_couchbase_user="couchbase"
project_application_couchbase_user_group="ninstall"
project_application_couchbase_user_home_dir="/home/couchbase"
project_application_www_data_user="www-data"
project_application_www_data_user_group="www-data"
project_application_www_data_user_home_dir="/home/www-data"
project_application_kafka_user="kafka"
project_application_kafka_user_group="ninstall"
project_application_kafka_user_home_dir="/home/kafka"
project_application_nagios_user="nagios"
project_application_nagios_user_group="nagios"
project_application_nagios_user_home_dir="/home/nagios"
project_application_spark_user="spark"
project_application_spark_user_group="spark"
project_application_spark_user_home_dir="/home/spark"
project_application_grafana_user="grafana"
project_application_grafana_user_group="grafana"
project_application_grafana_user_home_dir="/home/grafana"

#####  application_other_definition #####
project_cem_app_hosts=(smaster1.rijin.com smaster2.rijin.com)
project_application_license_user_group="ninstall"
project_ha_cluster_type="GLUSTER"
project_cluster_ha_status="yes"
project_is_etl_lic_required="no"
project_is_bapd_lic_required="yes"
project_application_master_node="smaster1.rijin.com"
project_new_app_node_addition="false"
bcmt_edge=""
project_release_name="ca4ci"

#####  kerberos_definition #####
project_common_kerberos_enable="yes"
project_common_kerberos_setup="yes"

#####  GLUSTER_definition #####
project_application_gluster_status="yes"
project_application_gluster_server_hosts=(smaster1.rijin.com smaster2.rijin.com sslave1.rijin.com)
project_application_mnt_gluster_client_hosts=(smaster1.rijin.com smaster2.rijin.com sslave1.rijin.com sslave2.rijin.com sslave3.rijin.com analytics.rijin.com)
project_application_postgres_gluster_client_hosts=()
project_application_gluster_postgres_mount_name="postgres_volume"
project_application_gluster_postgres_disk_name="postgres"
project_application_gluster_mnt_mount_name="mnt_volume"
project_application_gluster_mnt_disk_name="mnt"
project_application_gluster_portal_mount_name="portal_volume"
project_application_gluster_portal_disk_name="data"

#####  snmp_definition #####
project_snmp_status="no"
project_snmp_ip="127.0.0.1"
project_snmp_port="162"
project_snmp_community="2011#Sai#2025"

#####  smtp_definition #####
project_smtp_status="yes"
project_smtp_ip="10.130.128.21"
project_smtp_host="nsn.mail.com"
project_smtp_sender_emailIDs=(BETA_CDH_18SP2@rijin.com)
project_smtp_receiver_emailIDs=(shivaji.nimbalkar@rijin.com bliss.cyriac@rijin.com anand.omkara_reddy@rijin.com sowmya.kanatur@rijin.com)

#####  project_port_definition #####

#####  Backup_definition #####
project_backup_application_postgres_enable="yes"
project_application_postgres_retention="7"
project_application_postgres_encryption="yes"
project_application_postgres_path="/Data1/backup/postgres/"
project_application_postgres_prefix="postgres"
project_application_analytics_db_name="analytics-db"
project_application_postgres_schema="sai"
project_application_postgres_db_type="postgres"
project_application_boxi_path="/Data1/backup/boxi/"
project_application_boxi_encryption="yes"
project_application_boxi_retention="7"
project_backup_application_boxi_enable="yes"
project_application_boxi_filesystembackup="no"
project_application_application_boxi_biar="yes"
project_application_couchbase_path="/Data1/backup/couchbase/"
project_application_couchbase_prefix="couchase_backup"
project_application_couchbase_encryption="yes"
project_application_couchbase_retention="7"
project_backup_application_couchbase_enable="yes"
project_nagios_admin_ui_port="80"
project_ganglia_admin_ui_port="80"
project_admin_ui_port="80"
project_version_admin_ui_port="80"
project_licence_admin_ui_port="80"
project_cloudera_admin_ui_port="7180"
project_amq_admin_ui_port="8161"
project_bo_tomcat_port="8080"
project_couchbase_ui_port="8091"
project_kafka_ui_port="9100"
project_scheduler_ui_port="8443"

#####  project_common_license_definition #####
licence_file_path="/tmp/ngdb/licensefiles/"

#####  project_rpm_path_definition #####
rpm_dir_path="/tmp/ngdb/RPMs/"

#####  application_content_pack_definition #####
project_application_content_pack_OTT_status="yes"
project_application_content_pack_TNP_status="yes"
project_application_content_pack_OBR_status="yes"
project_application_content_pack_FIXEDLINE_status="yes"
project_application_content_pack_CEI2_status="yes"
project_application_content_pack_PROFILE_INDEX_status="yes"
project_application_content_pack_ANALYTICS_CLUSTERING_status="yes"
project_application_content_pack_CQI_status="yes"
project_application_content_pack_VIP_MONITORING_status="yes"
project_application_content_pack_ICE_status="no"
project_application_content_pack_FLEXIREPORT_CQI_status="yes"

#####  application_adaptation_definition #####
project_application_adaptation_Fixed15Min_status="yes"
project_application_adaptation_BCSI_status="yes"
project_application_adaptation_VOLTE_status="yes"
project_application_adaptation_ICEHiveToHbase_status="yes"
project_application_adaptation_HOTSPOT_status="yes"
project_application_adaptation_FixedBroadBand_status="yes"
project_application_adaptation_VideoAnalytics_status="yes"
project_application_adaptation_RADIO_status="yes"
project_application_adaptation_COMMON_status="yes"
project_application_adaptation_SQM_status="yes"
project_application_adaptation_CSI_status="yes"
project_application_adaptation_NE_status="yes"
project_application_adaptation_VOWIFI_status="yes"
project_application_adaptation_ICERadio_status="yes"
project_application_adaptation_MGW_status="yes"

#####  ice_topology #####
project_application_ice_topology_SMS_status="no"
project_application_ice_topology_S1MME_status="no"
project_application_ice_topology_VOLTE_TRAFFICA_status="no"
project_application_ice_topology_S1U_status="no"
project_application_ice_topology_FNG_FNS_4G_status="no"
project_application_ice_topology_ICE_RADIO_ENB_status="no"
project_application_ice_topology_SGSN_T_status="no"
project_application_ice_topology_ICE_RADIO_RNC_status="no"
project_application_ice_topology_GNUP_status="no"
project_application_ice_topology_PROBES_SGSN_IUPS_status="no"
project_application_ice_topology_VOLTE_PROBES_status="no"
project_application_ice_topology_PROBES_SGSN_GB_status="no"
project_application_ice_topology_FNG_status="no"
project_application_ice_topology_VOICE_status="no"
project_application_ice_topology_VOWIFI_TRAFFICA_status="no"
project_application_ice_topology_VOWIFI_PROBES_status="no"

#####  adapterframework #####
project_adapterframework_enable="no"

#####  shared_cluster_definition #####
kubernetes_controller_hosts=()
kubernetes_worker_hosts=()
kubernetes_edge_hosts=()
cdlk_control_hosts=(hmaster1.rijin.com hmaster2.rijin.com hutility1.rijin.com hutility2.rijin.com)
kubernetes_edge_ips=()
is_kube_cluster_enabled="no"
project_cloudera_mgr_host="hutility1.rijin.com"
project_cloudera_mgr_ip="10.53.74.129"
num_of_bcmt_worker_nodes=""
hive_metastore_fip=""
hive_metastore_dbname="db1"

#####  vault_definition #####
vault_secret_url="http://:8200/v1/secret/"
vault_enabled="no"
vault_token=""

#####  tls_definition #####
tls_enabled="yes"

#####  actionableInsight_definition #####
actionableinsight_enabled="no"

#####  jupyter definition #####
project_jupyter_status="no"
project_jupyter_hosts=()

#####  mongodb definition #####
project_mongodb_status="no"
project_mongodb_tls_status="no"
project_mongodb_encryption_status=""
project_configserver_hosts=(@)
project_shard_hosts=(@)
project_router_hosts=(@)
project_cnfsvr_path="/Data2/mongodb"
project_shard_path="/Data0/mongodb/"
project_mongod_port="27017"
project_router_port="27019"
project_mongodb_user="admin"
project_mongodb_passw="09771E11548C7E18CA9D862C709D9FA81381D88087F52BF7"

#####  timezone_definition #####
project_local_time_zone_enabled="no"
project_local_time_zone="Asia/Kolkata"

#####  deployment_definition #####
project_deployment_country="IN"
project_deployment_state="KAR"
project_deployment_locality_name="BLR"
project_deployment_organisation_name="RIJIN"
project_deployment_organisation_unit_name="PROJECT"
project_deployment_email_id="project@rijin.com"
