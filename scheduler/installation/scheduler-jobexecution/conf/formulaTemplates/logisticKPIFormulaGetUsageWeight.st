CASE 
WHEN ($USAGE_KPI$) = 0 AND $TARGET_KPI$ IS NOT NULL THEN $DEFAULT_VALUE$
WHEN $TARGET_KPI$ IS NULL THEN NULL
ELSE $FORMULA$
END