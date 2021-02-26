create table dq_rules_definition
  (
    rule_id integer PRIMARY KEY,
    rule_name varchar(100),
    rule_presentation_name varchar(200),
    rule_class varchar(100),
    default_value varchar(5000),
    dimension varchar(100),
    rule_description varchar(200),
    default_excludes varchar(1000)
);

create table dq_fields_definition
  (
    id integer PRIMARY KEY AUTOINCREMENT,
    table_name varchar(100),
    table_id numeric,
    column_name varchar(100),
    column_id numeric,
    computed_date timestamp,
    column_precision varchar(50),
    column_datatype varchar(50)
);

create table dq_fields_score
  (
    field_def_id integer unsigned,
    dimension varchar(100),
    ruleid integer,
    rule_parameter varchar(5000),
    bad_record_count numeric,
    source varchar(50),
    score numeric,
    weight integer,
    threshold numeric,
    check_pass varchar(50),
    start_time timestamp,
    end_time timestamp,
    rule_condition varchar(200),
    total_row_count numeric,
    CONSTRAINT chk_source CHECK (source IN ('CEM', 'CUSTOM', 'PRODUCT')),
    CONSTRAINT chk_pass CHECK (check_pass IN ('Y', 'N','NA')),
    FOREIGN KEY (field_def_id) REFERENCES DQ_FIELDS_DEFINITION (id) ON DELETE CASCADE
);


create table dq_kpi_cde_scores
  (
    object_id integer,
    object_type varchar(50),
    parent_object_id integer,
    kpi_or_cde_name varchar(200),
    kpi_or_cde_description varchar(1000),
    overall_score numeric,
    completeness_score numeric,
    uniqueness_score numeric,
    conformance_score numeric,
    integrity_score numeric,
    range_score numeric,
    consistence_score numeric,
    computed_date timestamp,
    load_date timestamp,
    CONSTRAINT chk_objType CHECK (object_type IN ('KPI', 'field'))
);

CREATE TABLE dq_kpi_definition
  (
id integer PRIMARY KEY AUTOINCREMENT,
    kpi_kqi_name varchar(100) UNIQUE
);


create table dq_score_calculator_trigger
  (
    trigger_script varchar(50),
    trigger_date timestamp

);

