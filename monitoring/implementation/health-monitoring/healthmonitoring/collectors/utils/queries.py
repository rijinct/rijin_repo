'''
Created on 30-Apr-2020

@author: khv
'''

from enum import Enum

from healthmonitoring.collectors.utils.connection import ConnectionFactory


class Queries(Enum):
    DISTINCT_JOB_NAMES = '''select distinct job_name from saischedule.qrtz_triggers where job_name like '%{input_arg}%' '''  # noqa: E501

    USAGE_ADAP_MAPPING = '''select 'us_' || lower(RTRIM(substr(jobid,7),'_LoadJob')) || '=' || adaptationid from sairepo.job_dictionary where jobid like 'Usage%LoadJob';'''  # noqa: E501

    LAST_HOUR_USAGE_JOBS = '''select count(distinct(job_name)) from sairepo.etl_status where start_time >= '{start_time}' and job_name like '{job_pattern}%' '''  # noqa: E501

    LAST_HOUR_USAGE_ERROR_JOBS = '''select count(distinct(job_name)) from sairepo.etl_status where start_time >= '{start_time}' and  status='E' and job_name like '{job_pattern}%' and error_description like '{error_description}' '''  # noqa: E501

    POSTGRES_USER_COUNT = '''select usename,count(*) from pg_stat_activity group by usename'''  # noqa: E501

    AGG_FAILED_JOBS = '''select job_name,start_time,end_time,status,error_description from sairepo.etl_status where status='E' and (job_name like '%{job_pattern}_AggregateJob' or job_name like '%{job_pattern}_ExportJob') and start_time >= '{start_time}' and end_time <= '{end_time}' '''  # noqa: E501

    USAGE_FAILED_JOBS = '''select job_name,start_time,end_time,status,error_description from sairepo.etl_status where status='E' and (job_name like '%{job_pattern}_LoadJob') and start_time >= '{start_time}' and end_time <= '{end_time}' '''  # noqa: E501

    USAGE_BOUNDARY = '''select a.jobid,maxvalue from (select boundary.jobid as jobid,maxvalue,region_id from sairepo.boundary where jobid not in (select jd.jobid from sairepo.job_dictionary jd join sairepo.job_prop jp on jd.id=jp.jobid and jp.paramname='ARCHIVING_DAYS' and jp.paramvalue='0' and jd.jobid like '%{input_arg}%') and maxvalue is not null) a where a.jobid like '%{input_arg}%' order by maxvalue desc '''  # noqa: E501

    AGG_BOUNDARY = '''select jobid, maxvalue as Aggregated_Till, case when now() < maxvalue + interval '2 days'  then 0 else floor(Delay) end as Cycles_Behind, (case when delay<0 then 'Disabled'  when jobid in (select jd.jobid from sairepo.job_dictionary jd join sairepo.job_prop jp on jd.id=jp.jobid and jp.paramname='JOB_ENABLED' and jp.paramvalue='NO' and jd.jobid like '%{input_arg}%_AggregateJob') then 'Disabled' else 'Enabled' end ) as Remarks from (select jobid,maxvalue,(DATE_PART('day', (now()) - (maxvalue + interval '1 days') ) + DATE_PART('hour', (now()) - (maxvalue + interval '1 days') )/24 + DATE_PART('minute', (now()) - (maxvalue + interval '1 days'))/(24*60)) Delay from sairepo.boundary where jobid like '%{input_arg}%_AggregateJob%' order by maxvalue desc) as a1;'''  # noqa: E501

    EXPECTED_DIMENSION_QS = '''select lower(SUBSTRING(SUBSTRING(job_name, 8, 45),1,length(SUBSTRING(job_name, 8, 45))-15)) as TableName,count(*) as ExpectedCount from ws_dimension_query_tab where job_name like '%{pattern}%' group by (job_name) order by lower(job_name) desc'''  # noqa: E501

    ACTUAL_DIMENSION_QS = '''select lower(SUBSTRING(table_name, 4,100)) as TableName,count(*) as ActualCount from cache_tab where source='D' and last_fetched_time >= 'startTime' and table_name like '%{pattern}%' group by (table_name) order by lower(table_name) desc'''  # noqa: E501

    EXPECTED_QS = '''select lower(aggregation_table_name), count(*) from saiws.ws_scheduled_cache_tab where aggregation_table_name like '%{pattern}%' and is_scheduled = '1' and (ws_request not like '%<from>LAST_WEEK_DAYS_FROM</from>%' or ws_request not like '%<from>MONTH_WEEK_NO_FROM</from>%') group by lower(aggregation_table_name) order by lower(aggregation_table_name) desc'''  # noqa: E501

    ACTUAL_QS = '''select table_name, count(*) as actual from saiws.cache_tab where source='S' and split_part(dt,',',1) >= '{unixtime}' and table_name ilike '%{pattern}%' group by table_name order by table_name'''  # noqa: E501

    HOUR_AGGREGATIONS = '''select '{last_hour}Hour==={current_hour}Hour' as Time_Frame,job_name,start_time,end_time,DATE_PART('hour', end_time - start_time )* 60 + DATE_PART('minute', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >'{date_hour}:00:00' and start_time <'{date_hour}:59:59' and job_name like '%HOUR_AggregateJob' order by end_time desc'''  # noqa: E501

    DAY_AGGREGATIONS = '''select {day} as Day,job_name,start_time,end_time,DATE_PART('hour', end_time - start_time )* 60 + DATE_PART('minute', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >'{curr_min_date}' and start_time <'{curr_max_date}' and (job_name like '%DAY_AggregateJob' or job_name like '%DAY_ExportJob') order by end_time desc'''  # noqa: E501

    WEEK_AGGREGATIONS = '''select '{week_start_day}Week==={week_end_day}Week' as Week,job_name,start_time,end_time,DATE_PART('hour', end_time - start_time )* 60 + DATE_PART('minute', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >'{curr_min_date}' and start_time <'{curr_max_date}' and job_name like '%WEEK_AggregateJob' order by end_time desc'''  # noqa: E501

    MONTH_AGGREGATIONS = '''select job_name,start_time,end_time,DATE_PART('hour', end_time - start_time )* 60 + DATE_PART('minute', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >'{curr_min_date}' and start_time <'{curr_max_date}' and job_name like '%MONTH_AggregateJob' order by executionduration  desc'''  # noqa: E501

    FIFTEEN_MIN_AGGREGATIONS = '''select '{last_hour}Hour==={current_hour}Hour' as Time_Frame,job_name,start_time,end_time,DATE_PART('hour', end_time - start_time )* 60 + DATE_PART('minute', end_time - start_time ) ExecutionDuration,status from sairepo.etl_status where start_time >'{date_hour}:00:00' and start_time <'{date_hour}:59:59' and job_name like '%15MIN_AggregateJob' order by executionduration  desc'''  # noqa: E501

    HOUR_JOB_COUNT_SUMMARY = '''select extract(hour from start_time) as HOUR, max(end_time) as HOUR_AGG_END_TIME, count(*) as TOTAL_JOBS_EXECUTED, COUNT(case when (end_time-start_time) = '00:00:00' then job_name end) as ZERO_DURATION, COUNT(case when status = 'S' then job_name end) as SUCCESS, COUNT(case when status = 'E' then job_name end) as ERROR, COUNT(case when status = 'R' then job_name end) as RUNNING from sairepo.etl_status where (job_name like '%HOUR%Aggre%' or job_name like '%HOUR_ExportJob') and start_time >'{curr_min_date}' and start_time <'{curr_max_date}' group by extract(hour from start_time) order by extract(hour from start_time)'''  # noqa: E501

    DAY_JOB_COUNT_SUMMARY = '''select extract(day from start_time) as DAY, max(end_time) as DAY_AGG_END_TIME, count(*) as TOTAL_JOBS_EXECUTED, COUNT(case when (end_time-start_time) = '00:00:00' then job_name end) as ZERO_DURATION, COUNT(case when status = 'S' then job_name end) as SUCCESS, COUNT(case when status = 'E' then job_name end) as ERROR, COUNT(case when status = 'R' then job_name end) as RUNNING from sairepo.etl_status where (job_name like '%DAY%Aggre%' or job_name like '%DAY_ExportJob') and start_time >'{curr_min_date}' and start_time <'{curr_max_date}' group by extract(day from start_time) order by extract(day from start_time)'''  # noqa: E501

    WEEK_JOB_COUNT_SUMMARY = '''select extract(week from start_time) as WEEK, max(end_time) as WEEK_AGG_END_TIME, count(*) as TOTAL_JOBS_EXECUTED, COUNT(case when (end_time-start_time) = '00:00:00' then job_name end) as ZERO_DURATION, COUNT(case when status = 'S' then job_name end) as SUCCESS, COUNT(case when status = 'E' then job_name end) as ERROR, COUNT(case when status = 'R' then job_name end) as RUNNING from sairepo.etl_status where job_name like '%WEEK_AggregateJob%' and start_time >'{curr_min_date}' and start_time <'{curr_max_date}' group by extract(week from start_time) order by extract(week from start_time)'''  # noqa: E501

    FIFTEEN_MIN_JOB_COUNT_SUMMARY = '''select extract(hour from start_time) as hour, max(end_time) as FMIN_AGG_END_TIME, count(*) as TOTAL_JOBS_EXECUTED, COUNT(case when (end_time-start_time) = '00:00:00' then job_name end) as ZERO_DURATION, COUNT(case when status = 'S' then job_name end) as SUCCESS, COUNT(case when status = 'E' then job_name end) as ERROR, COUNT(case when status = 'R' then job_name end) as RUNNING from sairepo.etl_status where job_name like '%15MIN%Aggre%' and start_time >'{curr_min_date}' and start_time <'{curr_max_date}' group by extract(hour from start_time) order by extract(hour from start_time)'''  # noqa: E501

    SDK_EXPORT_JOBS_INFO = '''select a.jobid,b.paramvalue from sairepo.job_dictionary a inner join (select jobid,paramvalue from sairepo.job_prop where jobid in (select jobid from sairepo.job_prop  where jobid in (select id from sairepo.job_dictionary where jobid like '%{pattern}%') and paramname='HIVE_TO_HBASE_LOADER' and paramvalue='false') and paramname='EXPORT_LOCATIONS') b on a.id=b.jobid'''  # noqa: E501

    TOPOLOGIES_DIR_INFO = '''select specid, mnt_column, ngdb_column, usage_job, plevel, right2.maxvalue from (select us.specid,ac.adaptationid||'_'||ac.version||'/'||us.specid||'_'||us.version as mnt_column,ac.adaptationid||'_'||ac.version||'/'||case when aus.specid is null then us.specid else aus.specid end||'_'||case when aus.specid is null then us.version else aus.version end as ngdb_column, 'Usage_'||case when aus.specid is null then us.specid else aus.specid end||'_1_LoadJob' usage_job from (select adaptationid as aer_adapid,usagespecid as aer_usid from sairepo.adap_entity_rel where adaptationid in (select id from sairepo.adapt_cata) and usagespecid is not null) join_table join sairepo.adapt_cata ac on join_table.aer_adapid=ac.id join (select * from sairepo.usage_spec where (abstract is null or abstract ='TRANSIENT') and version = '1') us on join_table.aer_usid=us.id left outer join sairepo.usage_spec_rel usr on usr.transientspec=us.id left outer join sairepo.usage_spec aus on aus.id=usr.virtualspec and aus.version = '1' order by us.specid)left1 join (select jd.jobid job, jp.paramvalue plevel from sairepo.job_dictionary jd join sairepo.job_prop jp on jd.id=jp.jobid and jp.paramname='PLEVEL' and jd.jobid like 'Usage%LoadJob')right1 on left1.usage_job=right1.job left outer join (select boundary.jobid as jobid,maxvalue from sairepo.boundary where jobid like '%Usage%') right2 on left1.usage_job=right2.jobid;'''  # noqa: E501

    MAPPERS_REDUCER = '''select load_time,job_id,report_time,time_zone,(stats ->> 'Total Mappers')::int AS Total_Mappers,(stats ->> 'Total Reducers')::int AS Total_reducers,stats ->> 'Total MapReduce CPU Time Spent' AS cpu_time,substring(stats ->> 'Total Time taken',0,position('seconds' in stats ->> 'Total Time taken'))::float/60 AS total_time,(stats ->> 'Records Inserted')::int AS Records_Inserted from sairepo.JOB_EXE_STATS where job_id like 'Perf%DAY%' and (load_time>{start_time} and load_time<={end_time} );'''  # noqa: E501

    LONG_RUNNING_JOB_QUERY = '''select (case when job_name like '%Export%' then 'Export' when job_name like '%QSJob%' then 'QS' else job_type end) job_type, job_name, start_time, end_time, duration from (select type as job_type, job_name, duration, start_time, end_time, (case when (type = '15MIN' and duration>={15min_long_duration}) or  (type = 'HOUR' and duration>={hour_long_duration}) or  ( type = 'DAY' and duration>={day_long_duration}) or  (type = 'WEEK' and duration>={week_long_duration}) or  ( type = 'MONTH' and duration>={month_long_duration}) then 'long' else null end) long_running_job from (select job_name, start_time, end_time, (case when job_name like '%Entity%' then 'DIMENSION' when job_name like '%Usage%' then 'USAGE' when job_name like '%15MIN%' then '15MIN' when job_name like '%HOUR%' then 'HOUR' when job_name like '%DAY%' then 'DAY' when job_name like '%WEEK%' then 'WEEK' when job_name like '%MONTH%' then 'MONTH' end) as type, (extract('day' from (end_time-start_time))*24*60 + extract('hour' from (end_time-start_time))*60 + extract('minute' from (end_time-start_time))) as duration from sairepo.etl_status where proc_id in (select proc_id from (select job_name, max(proc_id) proc_id from sairepo.etl_status where (job_name like '%AggregateJob%' or job_name like '%Export%' or job_name like '%QS%') and start_time >='{starts_time}' and status!='W' and status!='R' and status!='E' group by job_name) tmp)) a ) b where long_running_job is not null'''  # noqa: E501

    COLUMN_COUNT_QUERY = '''select count(*) from sairepo.char_spec join sairepo.usage_spec_char_use on char_spec.id=usage_spec_char_use.usagespeccharid where usage_spec_char_use.usagespecid=(select id from sairepo.usage_spec where specid ='{topology_name}' and version='1') and (upper(char_spec.abstract)='TRANSIENT' or char_spec.abstract is null)'''  # noqa: E50

    USAGE_COUNT_MON_QUERY = '''select hm_json from hm_stats where hm_type='usageTableCount' and json_value(hm_json,'$.Hour')=24 and hm_date>'{date_and_time}' '''  # noqa: E501

    TABLE_MAX_VALUE = '''select date(maxvalue) from sairepo.boundary where jobid like '{table_name}' '''  # noqa: E501


class HiveQueries(Enum):
    pass


class QueryReplacer:
    @staticmethod
    def get_replaced_sql(sql, **sql_args):
        return sql.value.format(**sql_args)


class QueryExecutor:
    @staticmethod
    def execute(db_type, query, **args):
        replaced_sql = QueryReplacer.get_replaced_sql(query, **args)
        connection = ConnectionFactory.get_connection(db_type)
        return connection.fetch_records(replaced_sql)
