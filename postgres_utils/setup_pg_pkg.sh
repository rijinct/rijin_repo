. ~/.bash_profile
source $NGDB_HOME/ifw/lib/application/utils/application_definition.sh

function install_pg_cron()
{
	File=/opt/nsn/ngdb/postgres-9.5/data/postgresql.conf
	count=`grep -c "pg_cron" "$File"`
	echo $count
	if [ $count -eq 1 ]; 
	then
		echo "pg_cron looks to be installed and configured, hence skipping"
	else
		echo "Installing pgcron"
		cd pg_cron-master
		export PATH=/usr/pgsql-9.5/bin/:$PATH
		make
		sudo PATH=$PATH make install
		sleep 5s
		cd /usr/pgsql-9.5/share/extension
		mv pg_cron--1.0.sql pg_cron--1.2.sql
		sed -i -e "\$ashared_preload_libraries = 'pg_cron'" $File
		sed -i -e "\$acron.database_name = 'sai'" $File
		
		tableExists=$(checkIfTableExists "cleanup_tables_metadata")
		if [[ "$tableExists" != "t" ]];then
			echo "Creating cron job"
			ssh $cemod_postgres_sdk_fip_active_host "su - postgres -c \"/opt/nsn/ngdb/pgsql/bin/psql sai -F, --no-align<<EOF;
			INSERT INTO cron.job (schedule, command, nodename, nodeport, database, username) VALUES ('0 20 * * *', 'select sairepo.func_cleanup()', 'localhost', 5432, 'sai', 'postgres');
			\q
			EOF\" "
			sh $cwd/create_psql_cleanup_metadata.sh
		else
			echo "Skipping cleanup table creation"
		fi
		create_extension pg_cron
	fi

}

function create_extension()
{
	echo "Creating pg_cron extension"
	ssh $cemod_postgres_sdk_fip_active_host "su - postgres -c \"/opt/nsn/ngdb/pgsql/bin/psql sai -F, --no-align<<EOF;
	CREATE EXTENSION $1;
	\q
	EOF\" "
	
	if [ $? != 0 ]; then
		echo "Error creating extention pg_cron"
		exit -1
	fi
}

function restart_postgres()
{
	perl /opt/nsn/ngdb/ifw/bin/security/generateChecksum.pl u
	if [ ${#cemod_postgres_ha_hosts[@]} -ge 2 ]
	then
		ssh ${cemod_postgres_ha_hosts[1]} "perl /opt/nsn/ngdb/ifw/bin/security/generateChecksum.pl u"
	fi

	echo "Restarting Postgres"
	perl $NGDB_HOME/ifw/bin/application/cem/application_services_controller.pl --service=postgres --level=stop 
	sleep 10s
	perl $NGDB_HOME/ifw/bin/application/cem/application_services_controller.pl --service=postgres --level=start
	sleep 10s
	if [ $? != 0 ]; then
		echo "Error restarting postgres"
		exit -1
	fi
}

function install_pgaudit()
{
	File="/opt/nsn/ngdb/postgres-9.5/data/postgresql.conf"
	count=`grep -c "pgaudit" "$File"`
	if [ $count -ne 0 ]; 
	then
		echo "pgaudit looks to be installed and configured, hence skipping"
		exit
	else
		echo "Installing pgaudit"
		cd pgaudit-1.0.8/
		export PATH=/usr/pgsql-9.5/bin/:$PATH
		make install USE_PGXS=1 PG_CONFIG=/usr/pgsql-9.5/bin/pg_config
		sudo PATH=$PATH make install USE_PGXS=1 PG_CONFIG=/usr/pgsql-9.5/bin/pg_config
		sleep 5s

		File="/opt/nsn/ngdb/postgres-9.5/data/postgresql.conf"
		sed -i -e "\$ashared_preload_libraries = 'pgaudit'" $File
		sed -i -e "\$apgaudit.log_catalog = off" $File
		sed -i -e "\$apgaudit.log = 'write, ddl'" $File
		sed -i -e "\$apgaudit.log_relation = 'on'" $File
		sed -i -e "\$apgaudit.log_parameter = 'on'" $File
		
		create_extension pagaudit

	fi

}


function checkIfTableExists()
{
table_name=$1
tableExists=`ssh ${cemod_postgres_sdk_fip_active_host} "/opt/nsn/ngdb/pgsql/bin/psql -U $cemod_application_postgres_user -d $cemod_sdk_db_name  -c \\"SELECT EXISTS (SELECT 1 FROM   information_schema.tables  WHERE  table_schema = '$cemod_sdk_schema_name' AND    table_name = '$table_name' );\\"" | egrep -v "row|--|exists" | tr -d [:space:]`
echo $tableExists
}

install_pg_cron

install_pgaudit

restart_postgres
