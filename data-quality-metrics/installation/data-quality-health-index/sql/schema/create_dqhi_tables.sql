CREATE TABLE SAIREPO.DQ_RULES_DEFINITION
  (
    rule_id INTEGER PRIMARY KEY,
    rule_name VARCHAR(100),
    rule_presentation_name VARCHAR(200),
    rule_class VARCHAR(100),
    default_value VARCHAR(5000),
    dimension VARCHAR(100),
    rule_description VARCHAR(200),
    default_excludes VARCHAR(1000)
);
GRANT ALL ON SAIREPO.DQ_RULES_DEFINITION to SAIREPO;

CREATE TABLE SAIREPO.DQ_FIELDS_DEFINITION
  (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    table_id NUMERIC,
    column_name VARCHAR(100),
    column_id NUMERIC,
    computed_date TIMESTAMP,
    column_precision VARCHAR(50),
    column_datatype VARCHAR(50)
);
GRANT ALL ON SAIREPO.DQ_FIELDS_DEFINITION to SAIREPO;

CREATE TABLE SAIREPO.DQ_FIELDS_SCORE
  (
    field_def_id INTEGER,
    dimension VARCHAR(100),
    ruleid INTEGER,
    rule_parameter VARCHAR(5000),
    bad_record_count NUMERIC,
    source VARCHAR(50),
    score NUMERIC,
    weight INTEGER,
    threshold NUMERIC,
    check_pass VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rule_condition VARCHAR(200),
    total_row_count NUMERIC,
    CONSTRAINT chk_source CHECK (source IN ('CEM', 'CUSTOM', 'PRODUCT')),
    CONSTRAINT chk_pass CHECK (check_pass IN ('Y', 'N','NA')),
    FOREIGN KEY (field_def_id) REFERENCES SAIREPO.DQ_FIELDS_DEFINITION (id) ON DELETE CASCADE
);
GRANT ALL ON SAIREPO.DQ_FIELDS_SCORE to SAIREPO;

CREATE TABLE SAIREPO.DQ_KPI_CDE_SCORES
  (
    object_id INTEGER,
    object_type VARCHAR(50),
    parent_object_id INTEGER,
    kpi_or_cde_name VARCHAR(200),
    kpi_or_cde_description VARCHAR(1000),
    overall_score NUMERIC,
    completeness_score NUMERIC,
    uniqueness_score NUMERIC,
    conformance_score NUMERIC,
    integrity_score NUMERIC,
    range_score NUMERIC,
    consistence_score NUMERIC,
    computed_date TIMESTAMP,
    load_date TIMESTAMP,
    CONSTRAINT chk_objType CHECK (object_type IN ('KPI', 'field'))
);
GRANT ALL ON SAIREPO.DQ_KPI_CDE_SCORES to SAIREPO;

CREATE TABLE SAIREPO.DQ_KPI_DEFINITION
  (
    id SERIAL NOT NULL,
    kpi_kqi_name VARCHAR(100) PRIMARY KEY
	);
GRANT ALL ON SAIREPO.DQ_KPI_DEFINITION to SAIREPO;
