CREATE TABLE saidata.es_sample_location_1 (
    cell_id numeric(10,0),
    sac_id numeric(10,0),
    cell_sac_id numeric(10,0),
    lac_id numeric(10,0),
    location_name character varying(50),
    latitude numeric(10,2),
    longitude numeric(10,2),
    city character varying(50),
    region character varying(50),
    mcc integer,
    mnc integer,
    test_time timestamp without time zone
);

ALTER TABLE saidata.es_sample_location_1 OWNER TO rithomas;

COMMENT ON COLUMN saidata.es_sample_location_1.cell_id IS 'SAMPLE_TYPE_ID';
COMMENT ON COLUMN saidata.es_sample_location_1.sac_id IS 'SAMPLE_TYPE';
COMMENT ON COLUMN saidata.es_sample_location_1.cell_sac_id IS 'DESCRIPTION';
REVOKE ALL ON TABLE saidata.es_sample_location_1 FROM PUBLIC;
REVOKE ALL ON TABLE saidata.es_sample_location_1 FROM rithomas;
GRANT ALL ON TABLE saidata.es_sample_location_1 TO rithomas;
GRANT ALL ON TABLE saidata.es_sample_location_1 TO saidata;


