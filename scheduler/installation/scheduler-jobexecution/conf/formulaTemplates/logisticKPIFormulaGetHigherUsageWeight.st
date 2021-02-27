CASE WHEN $TARGET_KPI$ IS NULL THEN NULL $USAGE_OBJ:{ obj | $if(obj.singleKPI)$ 
WHEN $first(obj.usageServiceWeightMap.keys)$ > 0 THEN $first(obj.usageServiceWeightMap.keys)$
$elseif(obj.twoKPIs)$ 
WHEN ($first(obj.usageServiceWeightMap.keys)$ > 0 AND $last(obj.usageServiceWeightMap.keys)$ > 0) THEN ($first(obj.usageServiceWeightMap.keys)$ * $first(obj.usageServiceWeightMap.values)$ + $last(obj.usageServiceWeightMap.keys)$ * $last(obj.usageServiceWeightMap.values)$)/($obj.usageServiceWeightMap.values;separator=" + "$)
$else$ 
WHEN ($trunc(obj.usageServiceWeightMap.keys);separator=" > 0 AND "$ > 0 AND $last(obj.usageServiceWeightMap.keys)$ > 0) THEN ($obj.usageServiceWeightMap.keys:{ k | $k$ * $obj.usageServiceWeightMap.(k)$};separator=" + "$)/($obj.usageServiceWeightMap.values;separator=" + "$)
$endif$ }$ 
ELSE NULL END