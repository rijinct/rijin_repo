from
(
$EACH_LEVEL_MAP.keys:{level_key| select * , $EACH_LEVEL_MAP.(level_key);separator=","$ from ( }$
select $KPI_COLUMN_LIST_EXCL_UDF;separator=","$ from (
select distinct $DIM_COLUMN_LIST;separator=","$ from 
$SOURCE_PERF_SPEC_PARAMS:{ obj |
 $if(obj.comma)$ UNION ALL $else$ ( $endif$ select  
}$
) unionResult) t0 
$SOURCE_PERF_SPEC_PARAMS:{ obj | left outer join 
$if(obj.transientSourceSpec)$
  ($obj.transientSelectStmt$
$else$
 (select 
