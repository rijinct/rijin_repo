from
(
$EACH_LEVEL_MAP.keys:{level_key| select * , $EACH_LEVEL_MAP.(level_key);separator=","$ from ( }$
select $KPI_COLUMN_LIST_EXCL_UDF;separator=","$ from (
$outer_select_list:{obj| select $obj.columnList;separator=","$ from ( }$ 
)tmp
$EACH_LEVEL_MAP.keys:{level_key| ) $level_key$}$
)outer_select 
$if(SUBPARTITION_TRUE)$ \$SubPartitionValues:{ obj | $endif$
