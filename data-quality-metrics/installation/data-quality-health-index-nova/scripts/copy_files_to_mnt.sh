dqhi_mnt_dir="/opt/nsn/ngdb/data-quality-metrics/data-quality-health-index/mnt"
conf_dir="${dqhi_mnt_dir}/conf"
output_dir="${dqhi_mnt_dir}/output"
if [ ! -d $conf_dir ]
then
	cp -r /ngdb/conf ${conf_dir}
	mkdir -p ${conf_dir}/custom ${output_dir}
	mkdir -p ${dqhi_mnt_dir}/log
	
fi