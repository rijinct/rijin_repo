$KpiObj:{ obj | $if(obj.commonDenom)$ pow( NVL( ( $endif$ $obj.kpiList:{kpiListObj | $if(!obj.commonDenom)$ pow( $endif$ if( $kpiListObj.sourceKpiInt$ is NULL or $kpiListObj.weight$ = 0, 0, $kpiListObj.weight$ ) $if(!obj.commonDenom)$ ,3) $endif$ $if(kpiListObj.check )$ + $endif$}$ $if(obj.commonDenom)$ ) / ( $obj.kpiList:{kpiListObj | if( $kpiListObj.sourceKpiInt$ is NULL or $kpiListObj.weight$ = 0, 0, 1) $if(kpiListObj.check)$ + $endif$}$ ) ,0 ),3) $endif$ $if(obj.check)$ + $endif$ }$