select distinct city,region from es_location_1 where city not in ('Unknown','NULL IN SOURCE');
