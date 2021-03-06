INSERT overwrite TABLE PS_VOLTE_FAIL_1_DAY partition (dt=''#LOWER_BOUND'' #TIMEZONE_PARTITION ,REGION)
SELECT VOLTE_FULL.SPARE_7,VOLTE_FULL.SPARE_8,VOLTE_FULL.CALL_DIRECTION,VOLTE_FULL.SPARE_9,VOLTE_FULL.IMSI,VOLTE_FULL.MSISDN,VOLTE_FULL.HANDSET_FULL_NAME,VOLTE_FULL.MARKETING_VENDOR,VOLTE_FULL.DEVICE_TYPE,VOLTE_FULL.DEVICE_CAPABILITY, VOLTE_FULL.QCI,VOLTE_FULL.CITY_ID,VOLTE_FULL.CITY,VOLTE_FULL.LOC_ID,VOLTE_FULL.LOCATION_NAME,VOLTE_FULL.spare_6,VOLTE_FULL.spare_2,VOLTE_FULL.CELL_SAC_NAME,VOLTE_FULL.LATITUDE,VOLTE_FULL.LONGITUDE,VOLTE_FULL.spare_5,VOLTE_FULL.spare_1,VOLTE_FULL.SUBS_TYPE,VOLTE_FULL.CUSTOMER_SEGMENTATION,VOLTE_FULL.GROUP_ID,VOLTE_FULL.GROUP_NAME,VOLTE_FULL.PARENT_GROUP_ID,VOLTE_FULL.PARENT_GROUP_NAME,VOLTE_FULL.ASSOCIATION_ID,VOLTE_FULL.ASSOCIATION_NAME,VOLTE_FULL.SUBSCRIPTION_PLAN,VOLTE_FULL.ROAMER_FLAG,VOLTE_FULL.RESPONSE_NAME,VOLTE_FULL.RESPONSE_ERROR_GROUP,VOLTE_FULL.RESPONSE_CODE,VOLTE_FULL.EPC_RESPONSE_CODE,VOLTE_FULL.EPC_RESPONSE_NAME,VOLTE_FULL.EPC_RESPONSE_ERROR_GROUP,VOLTE_FULL.SPARE_10,VOLTE_FULL.IMSI_ID,
NULL AS KPI_SPARE_1,
SUM(VOLTE_FULL.REG_FAILURES) AS REG_FAILURES,
NULL AS KPI_SPARE_2,
SUM(VOLTE_FULL.CALL_FAILURES) AS CALL_FAILURES,
SUM(VOLTE_FULL.CALL_SETUP_FAILURES) AS CALL_SETUP_FAILURES,
SUM(VOLTE_FULL.CALL_DROP) AS CALL_DROP,
NULL AS KPI_SPARE_3,
SUM(VOLTE_FULL.BEARER_ACT_FAILURES) AS BEARER_ACT_FAILURES,
NULL AS KPI_SPARE_4,
SUM(VOLTE_FULL.DEDICATED_ACT_FAILURES) AS DEDICATED_ACT_FAILURES,
NULL AS KPI_SPARE_5,
NULL AS KPI_SPARE_6,
NULL AS KPI_SPARE_7,
NULL AS KPI_SPARE_8,
NULL AS KPI_SPARE_9,
NULL AS KPI_SPARE_10,
NULL AS KPI_SPARE_11,
SUM(VOLTE_FULL.SRVCC_FAILURES) AS SRVCC_FAILURES,
NULL AS KPI_SPARE_12,
SUM(VOLTE_FULL.SRVCC_4G_2G_HO_FAILURES) AS SRVCC_4G_2G_HO_FAILURES,
NULL AS KPI_SPARE_13,
SUM(VOLTE_FULL.SRVCC_4G_3G_HO_FAILURES) AS SRVCC_4G_3G_HO_FAILURES,
VOLTE_FULL.OS_VERSION,VOLTE_FULL.OS_VENDOR,VOLTE_FULL.OPERATOR_NAME,VOLTE_FULL.COUNTRY_NAME,VOLTE_FULL.EVENT_GROUP_NAME,VOLTE_FULL.EPC_EVENT_GROUP_NAME,VOLTE_FULL.OPERATOR_ID,VOLTE_FULL.spare_3,VOLTE_FULL.HVC_FLAG,VOLTE_FULL.subscription_planb_details,NULL as CGI_ID,VOLTE_FULL.mcc_mnc_lac_cell_sac,VOLTE_FULL.azimuth_angle,
SUM(VOLTE_FULL.CALL_DROP_RINGING) AS CALL_DROP_RINGING,
VOLTE_FULL.RAN_OPERATOR,
case when substr(response_code,1,3)=''555'' then 
case 
when VOLTE_FULL.EQUIVALENT_EVENT_NAME=''Default Bearer Activation'' then ''Create Session''
when VOLTE_FULL.EQUIVALENT_EVENT_NAME=''Dedicated Bearer Activation'' then ''Create Bearer''
end 
else VOLTE_FULL.EQUIVALENT_EVENT_NAME
end as EQUIVALENT_EVENT_NAME,
VOLTE_FULL.EPC_EQUIVALENT_EVENT_NAME,
SUM(RE_REGISTRATION_FAILURES) AS RE_REGISTRATION_FAILURES,
SUM(INTRA_S1_HO_FAILURES) AS INTRA_S1_HO_FAILURES,
SUM(PATH_SWITCH_HO_FAILURES) AS PATH_SWITCH_HO_FAILURES,
VOLTE_FULL.REGION
FROM
(SELECT NULL AS SPARE_7,
NULL AS SPARE_8,
CALL_DIRECTION AS CALL_DIRECTION,
NULL AS SPARE_9,
IMSI AS IMSI,
MSISDN AS MSISDN,
HANDSET_FULL_NAME AS HANDSET_FULL_NAME,
MARKETING_VENDOR AS MARKETING_VENDOR,
DEVICE_TYPE AS DEVICE_TYPE,
DEVICE_CAPABILITY AS DEVICE_CAPABILITY,
QCI AS QCI,
REGION AS REGION,
CITY_ID AS CITY_ID,
CITY AS CITY,
LOC_ID AS LOC_ID,
LOCATION_NAME AS LOCATION_NAME,
NULL as spare_6,
NULL  AS  spare_2,
CELL_SAC_NAME AS CELL_SAC_NAME,
LATITUDE AS  LATITUDE,
LONGITUDE AS  LONGITUDE,
NULL AS  spare_5,
NULL AS  spare_1,
SUBS_TYPE AS SUBS_TYPE,
CUSTOMER_SEGMENTATION AS CUSTOMER_SEGMENTATION,
GROUP_ID AS GROUP_ID,
GROUP_NAME AS GROUP_NAME,
PARENT_GROUP_ID AS PARENT_GROUP_ID,
PARENT_GROUP_NAME AS PARENT_GROUP_NAME,
ASSOCIATION_ID AS ASSOCIATION_ID,
ASSOCIATION_NAME AS ASSOCIATION_NAME,
SUBSCRIPTION_PLAN AS SUBSCRIPTION_PLAN,
ROAMER_FLAG  AS  ROAMER_FLAG,
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then EPC_RESPONSE_NAME else RESPONSE_NAME end AS RESPONSE_NAME, 
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then EPC_RESPONSE_ERROR_GROUP else RESPONSE_ERROR_GROUP end AS RESPONSE_ERROR_GROUP, 
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then EPC_RESPONSE_CODE else RESPONSE_CODE end AS RESPONSE_CODE, 
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then NULL else decode(EPC_RESPONSE_CODE,-10,NULL,-1,NULL,EPC_RESPONSE_CODE) end  AS EPC_RESPONSE_CODE,
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then NULL else decode(EPC_RESPONSE_NAME,''Unknown'',NULL,''NULL IN SOURCE'',NULL,EPC_RESPONSE_NAME) end AS EPC_RESPONSE_NAME,
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then NULL else decode(EPC_RESPONSE_ERROR_GROUP,-10,NULL,-1,NULL,EPC_RESPONSE_ERROR_GROUP) end  as EPC_RESPONSE_ERROR_GROUP,
NULL    AS  SPARE_10,
IMSI_ID      AS  IMSI_ID,
VOLTE_REG_FAILURES AS REG_FAILURES,
VOLTE_CALL_FAILURES AS CALL_FAILURES,
VOLTE_CALL_SETUP_FAILURES AS CALL_SETUP_FAILURES,
VOLTE_CALL_DROP AS CALL_DROP,
VOLTE_BEARER_ACT_FAILURES AS BEARER_ACT_FAILURES,
VOLTE_DEDICATED_ACT_FAILURES AS DEDICATED_ACT_FAILURES,   
SRVCC_FAILURES AS SRVCC_FAILURES,
SRVCC_4G_2G_HO_FAILURES AS SRVCC_4G_2G_HO_FAILURES,
SRVCC_4G_3G_HO_FAILURES AS SRVCC_4G_3G_HO_FAILURES,
OS_VERSION AS OS_VERSION,
OS_VENDOR AS OS_VENDOR,
OPERATOR_NAME AS OPERATOR_NAME,
COUNTRY_NAME AS COUNTRY_NAME,
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then EPC_EVENT_GROUP_NAME else EVENT_GROUP_NAME end
AS EVENT_GROUP_NAME,
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then NULL else decode(EPC_EVENT_GROUP_NAME,''Unknown'',NULL,''NULL IN SOURCE'',NULL,EPC_EVENT_GROUP_NAME) end AS EPC_EVENT_GROUP_NAME,
OPERATOR_ID AS OPERATOR_ID,
NULL AS spare_3,
HVC_FLAG AS HVC_FLAG,
subscription_planb_details as subscription_planb_details,
CGI_ID AS CGI_ID,
  concat(mcc,''/'',mnc,''/'',lac_id,''/'',cell_sac_id) AS mcc_mnc_lac_cell_sac,
azimuth_angle AS azimuth_angle,
VOLTE_CALL_DROP_RINGING AS CALL_DROP_RINGING,
RAN_OPERATOR as RAN_OPERATOR,
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then EPC_EQUIVALENT_EVENT_NAME else EQUIVALENT_EVENT_NAME end AS EQUIVALENT_EVENT_NAME,  
case when EPC_EQUIVALENT_EVENT_NAME in  (''Default Bearer Activation'',''Dedicated Bearer Activation'') and epc_category_id=7 then NULL else decode(EPC_EQUIVALENT_EVENT_NAME,''Unknown'',NULL,''NULL IN SOURCE'',NULL,EPC_EQUIVALENT_EVENT_NAME) end  AS EPC_EQUIVALENT_EVENT_NAME ,
VOLTE_RE_REGISTRATION_FAILURES AS RE_REGISTRATION_FAILURES,
VOLTE_INTRA_S1_HO_FAILURES AS INTRA_S1_HO_FAILURES,
VOLTE_PATH_SWITCH_HO_FAILURES AS PATH_SWITCH_HO_FAILURES
FROM PS_VOLTE_SEGG_1_DAY PS
WHERE PS.dt >= ''#LOWER_BOUND'' AND PS.dt< ''#UPPER_BOUND'' #TIMEZONE_CHECK
AND (PS.VOLTE_CALL_FAILURES+PS.VOLTE_REG_FAILURES+PS.VOLTE_BEARER_ACT_FAILURES+PS.VOLTE_DEDICATED_ACT_FAILURES+PS.SRVCC_FAILURES+VOLTE_RE_REGISTRATION_FAILURES+PS.VOLTE_INTRA_S1_HO_FAILURES+PS.VOLTE_PATH_SWITCH_HO_FAILURES) > 0 AND nvl(PS.MVNO_ID,-1) = -1
) VOLTE_FULL
GROUP BY VOLTE_FULL.SPARE_7,VOLTE_FULL.SPARE_8,VOLTE_FULL.CALL_DIRECTION,VOLTE_FULL.SPARE_9,VOLTE_FULL.IMSI,VOLTE_FULL.MSISDN,VOLTE_FULL.HANDSET_FULL_NAME,VOLTE_FULL.MARKETING_VENDOR,VOLTE_FULL.DEVICE_TYPE,VOLTE_FULL.DEVICE_CAPABILITY, VOLTE_FULL.QCI,VOLTE_FULL.REGION,VOLTE_FULL.CITY_ID,VOLTE_FULL.CITY,VOLTE_FULL.LOC_ID,VOLTE_FULL.LOCATION_NAME,VOLTE_FULL.spare_6,VOLTE_FULL.spare_2,VOLTE_FULL.CELL_SAC_NAME,VOLTE_FULL.LATITUDE,VOLTE_FULL.LONGITUDE,VOLTE_FULL.spare_5,VOLTE_FULL.spare_1,VOLTE_FULL.SUBS_TYPE,VOLTE_FULL.CUSTOMER_SEGMENTATION,VOLTE_FULL.GROUP_ID,VOLTE_FULL.GROUP_NAME,VOLTE_FULL.PARENT_GROUP_ID,VOLTE_FULL.PARENT_GROUP_NAME,VOLTE_FULL.ASSOCIATION_ID,VOLTE_FULL.ASSOCIATION_NAME,VOLTE_FULL.SUBSCRIPTION_PLAN,VOLTE_FULL.ROAMER_FLAG,VOLTE_FULL.RESPONSE_NAME,VOLTE_FULL.RESPONSE_ERROR_GROUP,VOLTE_FULL.RESPONSE_CODE,VOLTE_FULL.EPC_RESPONSE_CODE,VOLTE_FULL.EPC_RESPONSE_NAME,VOLTE_FULL.EPC_RESPONSE_ERROR_GROUP,VOLTE_FULL.SPARE_10,VOLTE_FULL.IMSI_ID,VOLTE_FULL.OS_VERSION,VOLTE_FULL.OS_VENDOR,VOLTE_FULL.OPERATOR_NAME,VOLTE_FULL.COUNTRY_NAME,VOLTE_FULL.EVENT_GROUP_NAME,VOLTE_FULL.EPC_EVENT_GROUP_NAME,VOLTE_FULL.OPERATOR_ID,VOLTE_FULL.spare_3,VOLTE_FULL.HVC_FLAG,VOLTE_FULL.subscription_planb_details,VOLTE_FULL.mcc_mnc_lac_cell_sac,VOLTE_FULL.azimuth_angle,VOLTE_FULL.RAN_OPERATOR,case when substr(response_code,1,3)=''555'' then  case  when VOLTE_FULL.EQUIVALENT_EVENT_NAME=''Default Bearer Activation'' then ''Create Session'' when VOLTE_FULL.EQUIVALENT_EVENT_NAME=''Dedicated Bearer Activation'' then ''Create Bearer'' end  else VOLTE_FULL.EQUIVALENT_EVENT_NAME end,
VOLTE_FULL.EPC_EQUIVALENT_EVENT_NAME distribute by (REGION) 