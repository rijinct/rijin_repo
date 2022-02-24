COPY (select xmlelement(name "dataQualityResult",(select xmlelement(name "resultList",(select XMLAGG(XMLELEMENT(name statistics, xmlforest(p.object_type as "objectType",p.parentObjectID as "parentObjectID",p.schemaname as "schemaName",p.databasetype as "databaseType",object_id as "objectID", upper(kpi_or_cde_name) as "objectName", kpi_or_cde_description as "objectDescription", overall_score as "score",completeness_score as "completenessScore",conformity_score as "conformityScore",consistency_score as consistencyScore,integrity_score as "integrityScore",range_score as "rangeScore",uniqueness_score as "uniquenessScore"))) from (select object_type,((case when object_type = 'KPI' then ''  else CAST(parent_object_id AS varchar) end)) as "parentobjectid",'PROJECT' as "schemaname", 'BIGDATA' as "databasetype",object_id, kpi_or_cde_name, kpi_or_cde_description, ((case when overall_score = '-1' then 'NA'  else CAST(overall_score AS varchar) end)) as overall_score, ((case when completeness_score = '-1' then 'NA'  else CAST(completeness_score AS varchar) end)) as "completeness_score",((case when conformance_score = '-1' then 'NA'  else CAST(conformance_score AS varchar) end)) as "conformity_score",((case when consistence_score = '-1' then 'NA'  else CAST(consistence_score AS varchar) end)) as "consistency_score",((case when integrity_score = '-1' then 'NA'  else CAST(integrity_score AS varchar) end)) as "integrity_score",((case when range_score = '-1' then 'NA'  else CAST(range_score AS varchar) end)) as "range_score",((case when uniqueness_score = '-1' then 'NA'  else CAST(uniqueness_score AS varchar) end)) as "uniqueness_score" from dq_kpi_cde_scores where computed_date='DATA_DATE' and load_date=(select max(load_date) from dq_kpi_cde_scores))p))q))) TO STDOUT