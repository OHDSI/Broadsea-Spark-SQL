-- note. database and schema are synonymous in Spark SQL
create database if not exists eunomia;

use eunomia;

-- note. OpenCSVSerde ignores hive tblproperties 'serialization.null.format'='' 
-- so we have to convert empty string to null for each nullable column in the later DELTA table insert select

-- note. can only use 'CREATE OR REPLACE' for v2 (DELTA) tables so explicit drops are needed for v1 tables

-----------------------------------------------
-- PERSON
-----------------------------------------------

DROP TABLE IF EXISTS eunomia.person_temp;
CREATE TABLE eunomia.person_temp (
			person_id INT,
			gender_concept_id INT,
			year_of_birth INT,
			month_of_birth INT, -- NULL
			day_of_birth INT, -- NULL
			birth_datetime TIMESTAMP, -- NULL
			race_concept_id INT,
			ethnicity_concept_id INT,
			location_id INT, -- NULL
			provider_id INT, -- NULL
			care_site_id INT, -- NULL
			person_source_value VARCHAR(50), -- NULL
			gender_source_value VARCHAR(50), -- NULL
			gender_source_concept_id INT, -- NULL
			race_source_value VARCHAR(50), -- NULL
			race_source_concept_id INT, -- NULL
			ethnicity_source_value VARCHAR(50), -- NULL
			ethnicity_source_concept_id INT -- NULL 
			)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');
			
LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/person.csv' OVERWRITE INTO TABLE eunomia.person_temp;

CREATE OR REPLACE TABLE eunomia.person USING DELTA AS SELECT * from eunomia.person_temp where 1 <> 1;
INSERT INTO eunomia.person
SELECT 
  person_id, 
  gender_concept_id, 
  year_of_birth, 
  nullif(month_of_birth, '') as month_of_birth, 
  nullif(day_of_birth, '') as day_of_birth, 
  birth_datetime, 
  race_concept_id, 
  ethnicity_concept_id, 
  nullif(location_id, '') as location_id, 
  nullif(provider_id, '') as provider_id, 
  nullif(care_site_id, '') as care_site_id, 
  nullif(person_source_value, '') as person_source_value, 
  nullif(gender_source_value, '') as gender_source_value, 
  nullif(gender_source_concept_id, '') as gender_source_concept_id, 
  nullif(race_source_value, '') as race_source_value, 
  nullif(race_source_concept_id, '') as race_source_concept_id, 
  nullif(ethnicity_source_value, '') as ethnicity_source_value, 
  nullif(ethnicity_source_concept_id, '') as ethnicity_source_concept_id
FROM eunomia.person_temp WHERE upper(person_id) <> 'PERSON_ID';

DROP TABLE IF EXISTS eunomia.person_temp;

-----------------------------------------------
-- OBSERVATION_PERIOD
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.observation_period_temp;
CREATE TABLE eunomia.observation_period_temp (
  observation_period_id INT,
  person_id INT,
  observation_period_start_date DATE,
  observation_period_end_date DATE,
  period_type_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/observation_period.csv' OVERWRITE INTO TABLE eunomia.observation_period_temp;

CREATE OR REPLACE TABLE eunomia.observation_period USING DELTA AS SELECT * from eunomia.observation_period_temp where 1 <> 1;
INSERT INTO eunomia.observation_period
SELECT
  observation_period_id,
  person_id,
  observation_period_start_date,
  observation_period_end_date,
  period_type_concept_id
FROM eunomia.observation_period_temp
where upper(observation_period_id) <> 'OBSERVATION_PERIOD_ID';

DROP TABLE IF EXISTS eunomia.observation_period_temp;

-----------------------------------------------
-- VISIT_OCCURRENCE
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.visit_occurrence_temp;
CREATE TABLE eunomia.visit_occurrence_temp (
  visit_occurrence_id INT,
  person_id INT,
  visit_concept_id INT,
  visit_start_date DATE,
  visit_start_datetime TIMESTAMP,
  visit_end_date DATE,
  visit_end_datetime TIMESTAMP,
  visit_type_concept_id INT,
  provider_id INT,
  care_site_id INT,
  visit_source_value VARCHAR(50),
  visit_source_concept_id INT,
  admitted_from_concept_id INT,
  admitted_from_source_value VARCHAR(50),
  discharged_to_concept_id INT,
  discharged_to_source_value VARCHAR(50),
  preceding_visit_occurrence_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/visit_occurrence.csv' OVERWRITE INTO TABLE eunomia.visit_occurrence_temp;

CREATE OR REPLACE TABLE eunomia.visit_occurrence USING DELTA AS SELECT * from eunomia.visit_occurrence_temp where 1 <> 1;
INSERT INTO eunomia.visit_occurrence
SELECT
  visit_occurrence_id,
  person_id,
  visit_concept_id,
  visit_start_date,
  visit_start_datetime,
  visit_end_date,
  visit_end_datetime,
  visit_type_concept_id,
  nullif(provider_id, '') as provider_id,
  nullif(care_site_id, '') as care_site_id,
  nullif(visit_source_value, '') as visit_source_value,
  nullif(visit_source_concept_id, '') as visit_source_concept_id,
  nullif(admitted_from_concept_id, '') as admitted_from_concept_id,
  nullif(admitted_from_source_value, '') as admitted_from_source_value,
  nullif(discharged_to_concept_id, '') as discharged_to_concept_id,
  nullif(discharged_to_source_value, '') as discharged_to_source_value,
  nullif(preceding_visit_occurrence_id, '') as preceding_visit_occurrence_id
FROM eunomia.visit_occurrence_temp WHERE upper(visit_occurrence_id) <> 'VISIT_OCCURRENCE_ID';

DROP TABLE IF EXISTS eunomia.visit_occurrence_temp;

-----------------------------------------------
-- VISIT_DETAIL
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.visit_detail_temp;
CREATE TABLE eunomia.visit_detail_temp (
  visit_detail_id INT,
  person_id INT,
  visit_detail_concept_id INT,
  visit_detail_start_date DATE,
  visit_detail_start_datetime TIMESTAMP,
  visit_detail_end_date DATE,
  visit_detail_end_datetime TIMESTAMP,
  visit_detail_type_concept_id INT,
  provider_id INT,
  care_site_id INT,
  visit_detail_source_value VARCHAR(50),
  visit_detail_source_concept_id INT,
  admitted_from_concept_id INT,
  admitted_from_source_value VARCHAR(50),
  discharged_to_source_value VARCHAR(50),
  discharged_to_concept_id INT,
  preceding_visit_detail_id INT,
  parent_visit_detail_id INT,
  visit_occurrence_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/visit_detail.csv' OVERWRITE INTO TABLE eunomia.visit_detail_temp;

CREATE OR REPLACE TABLE eunomia.visit_detail USING DELTA AS SELECT * from eunomia.visit_detail_temp where 1 <> 1;
INSERT INTO eunomia.visit_detail
SELECT
  visit_detail_id,
  person_id,
  visit_detail_concept_id,
  visit_detail_start_date,
  visit_detail_start_datetime,
  visit_detail_end_date,
  visit_detail_end_datetime,
  visit_detail_type_concept_id,
  nullif(provider_id, '') as provider_id,
  nullif(care_site_id, '') as care_site_id,
  nullif(visit_detail_source_value, '') as visit_detail_source_value,
  nullif(visit_detail_source_concept_id, '') as visit_detail_source_concept_id,
  nullif(admitted_from_concept_id, '') as admitted_from_concept_id,
  nullif(admitted_from_source_value, '') as admitted_from_source_value,
  nullif(discharged_to_source_value, '') as discharged_to_source_value,
  nullif(discharged_to_concept_id, '') as discharged_to_concept_id,
  nullif(preceding_visit_detail_id, '') as preceding_visit_detail_id,
  nullif(parent_visit_detail_id, '') as parent_visit_detail_id,
  visit_occurrence_id
FROM eunomia.visit_detail_temp WHERE upper(visit_detail_id) <> 'VISIT_DETAIL_ID';

DROP TABLE IF EXISTS eunomia.visit_detail_temp;

-----------------------------------------------
-- CONDITION_OCCURRENCE
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.condition_occurrence_temp;
CREATE TABLE eunomia.condition_occurrence_temp (
  condition_occurrence_id INT,
  person_id INT,
  condition_concept_id INT,
  condition_start_date DATE,
  condition_start_datetime TIMESTAMP,
  condition_end_date DATE,
  condition_end_datetime TIMESTAMP,
  condition_type_concept_id INT,
  condition_status_concept_id INT,
  stop_reason VARCHAR(20),
  provider_id INT,
  visit_occurrence_id INT,
  visit_detail_id INT,
  condition_source_value VARCHAR(50),
  condition_source_concept_id INT,
  condition_status_source_value VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/condition_occurrence.csv' OVERWRITE INTO TABLE eunomia.condition_occurrence_temp;

CREATE OR REPLACE TABLE eunomia.condition_occurrence USING DELTA AS SELECT * from eunomia.condition_occurrence_temp where 1 <> 1;
INSERT INTO eunomia.condition_occurrence
SELECT
  condition_occurrence_id,
  person_id,
  condition_concept_id,
  condition_start_date,
  condition_start_datetime,
  condition_end_date,
  condition_end_datetime,
  condition_type_concept_id,
  nullif(condition_status_concept_id, '') as condition_status_concept_id,
  nullif(stop_reason, '') as stop_reason,
  nullif(provider_id, '') as provider_id,
  nullif(visit_occurrence_id, '') as visit_occurrence_id,
  nullif(visit_detail_id, '') as visit_detail_id,
  nullif(condition_source_value, '') as condition_source_value,
  nullif(condition_source_concept_id, '') as condition_source_concept_id,
  nullif(condition_status_source_value, '') as condition_status_source_value
FROM eunomia.condition_occurrence_temp WHERE upper(condition_occurrence_id) <> 'CONDITION_OCCURRENCE_ID';

DROP TABLE IF EXISTS eunomia.condition_occurrence_temp;

-----------------------------------------------
-- DRUG_EXPOSURE
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.drug_exposure_temp;
CREATE TABLE eunomia.drug_exposure_temp (
  drug_exposure_id INT,
  person_id INT,
  drug_concept_id INT,
  drug_exposure_start_date DATE,
  drug_exposure_start_datetime TIMESTAMP,
  drug_exposure_end_date DATE,
  drug_exposure_end_datetime TIMESTAMP,
  verbatim_end_date DATE,
  drug_type_concept_id INT,
  stop_reason VARCHAR(20),
  refills INT,
  quantity NUMERIC,
  days_supply INT,
  sig STRING,
  route_concept_id INT,
  lot_number VARCHAR(50),
  provider_id INT,
  visit_occurrence_id INT,
  visit_detail_id INT,
  drug_source_value VARCHAR(50),
  drug_source_concept_id INT,
  route_source_value VARCHAR(50),
  dose_unit_source_value VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/drug_exposure.csv' OVERWRITE INTO TABLE eunomia.drug_exposure_temp;

CREATE OR REPLACE TABLE eunomia.drug_exposure USING DELTA AS SELECT * from eunomia.drug_exposure_temp where 1 <> 1;
INSERT INTO eunomia.drug_exposure
SELECT
  drug_exposure_id,
  person_id,
  drug_concept_id,
  drug_exposure_start_date,
  drug_exposure_start_datetime,
  drug_exposure_end_date,
  drug_exposure_end_datetime,
  verbatim_end_date,
  drug_type_concept_id,
  nullif(stop_reason, '') as stop_reason,
  nullif(refills, '') as refills,
  nullif(quantity, '') as quantity,
  nullif(days_supply, '') as days_supply,
  nullif(sig, '') as sig,
  nullif(route_concept_id, '') as route_concept_id,
  nullif(lot_number, '') as lot_number,
  nullif(provider_id, '') as provider_id,
  nullif(visit_occurrence_id, '') as visit_occurrence_id,
  nullif(visit_detail_id, '') as visit_detail_id,
  nullif(drug_source_value, '') as drug_source_value,
  nullif(drug_source_concept_id, '') as drug_source_concept_id,
  nullif(route_source_value, '') as route_source_value,
  nullif(dose_unit_source_value, '') as dose_unit_source_value
FROM eunomia.drug_exposure_temp WHERE upper(drug_exposure_id) <> 'DRUG_EXPOSURE_ID';

DROP TABLE IF EXISTS eunomia.drug_exposure_temp;

-----------------------------------------------
-- PROCEDURE_OCCURRENCE
-----------------------------------------------
drop table if exists eunomia.procedure_occurrence_temp;
create table eunomia.procedure_occurrence_temp (
  procedure_occurrence_id INT,
  person_id INT,
  procedure_concept_id INT,
  procedure_date DATE,
  procedure_datetime TIMESTAMP,
  procedure_end_date DATE,
  procedure_end_datetime TIMESTAMP,
  procedure_type_concept_id INT,
  modifier_concept_id INT,
  quantity INT,
  provider_id INT,
  visit_occurrence_id INT,
  visit_detail_id INT,
  procedure_source_value VARCHAR(50),
  procedure_source_concept_id INT,
  modifier_source_value VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/procedure_occurrence.csv' OVERWRITE INTO TABLE eunomia.procedure_occurrence_temp;

CREATE OR REPLACE TABLE eunomia.procedure_occurrence USING DELTA AS SELECT * from eunomia.procedure_occurrence_temp where 1 <> 1;
INSERT INTO eunomia.procedure_occurrence
SELECT
  procedure_occurrence_id,
  person_id,
  procedure_concept_id,
  procedure_date,
  procedure_datetime,
  procedure_end_date,
  procedure_end_datetime,
  procedure_type_concept_id,
  nullif(modifier_concept_id, '') as modifier_concept_id,
  nullif(quantity, '') as quantity,
  nullif(provider_id, '') as provider_id,
  nullif(visit_occurrence_id, '') as visit_occurrence_id,
  nullif(visit_detail_id, '') as visit_detail_id,
  nullif(procedure_source_value, '') as procedure_source_value,
  nullif(procedure_source_concept_id, '') as procedure_source_concept_id,
  nullif(modifier_source_value, '') as modifier_source_value
FROM eunomia.procedure_occurrence_temp WHERE upper(procedure_occurrence_id) <> 'PROCEDURE_OCCURRENCE_ID';

DROP TABLE IF EXISTS eunomia.procedure_occurrence_temp;

-----------------------------------------------
-- DEVICE_EXPOSURE
-----------------------------------------------

drop table if exists eunomia.device_exposure_temp;

create table eunomia.device_exposure_temp (
  device_exposure_id INT,
  person_id INT,
  device_concept_id INT,
  device_exposure_start_date DATE,
  device_exposure_start_datetime TIMESTAMP,
  device_exposure_end_date DATE,
  device_exposure_end_datetime TIMESTAMP,
  device_type_concept_id INT,
  unique_device_id VARCHAR(255),
  production_id VARCHAR(255),
  quantity INT,
  provider_id INT,
  visit_occurrence_id INT,
  visit_detail_id INT,
  device_source_value VARCHAR(50),
  device_source_concept_id INT,
  unit_concept_id INT,
  unit_source_value VARCHAR(50),
  unit_source_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/device_exposure.csv' OVERWRITE INTO TABLE eunomia.device_exposure_temp;

CREATE OR REPLACE TABLE eunomia.device_exposure USING DELTA AS SELECT * from eunomia.device_exposure_temp where 1 <> 1;
INSERT INTO eunomia.device_exposure
SELECT
  device_exposure_id,
  person_id,
  device_concept_id,
  device_exposure_start_date,
  device_exposure_start_datetime,
  device_exposure_end_date,
  device_exposure_end_datetime,
  device_type_concept_id,
  nullif(unique_device_id, '') as unique_device_id,
  nullif(production_id, '') as production_id,
  nullif(quantity, '') as quantity,
  nullif(provider_id, '') as provider_id,
  nullif(visit_occurrence_id, '') as visit_occurrence_id,
  nullif(visit_detail_id, '') as visit_detail_id,
  nullif(device_source_value, '') as device_source_value,
  nullif(device_source_concept_id, '') as device_source_concept_id,
  nullif(unit_concept_id, '') as unit_concept_id,
  nullif(unit_source_value, '') as unit_source_value,
  nullif(unit_source_concept_id, '') as unit_source_concept_id
FROM eunomia.device_exposure_temp WHERE upper(device_exposure_id) <> 'DEVICE_EXPOSURE_ID';

DROP TABLE IF EXISTS eunomia.device_exposure_temp;

-----------------------------------------------
-- MEASUREMENT
-----------------------------------------------
drop table if exists eunomia.measurement_temp;

create table eunomia.measurement_temp (
  measurement_id INT,
  person_id INT,
  measurement_concept_id INT,
  measurement_date DATE,
  measurement_datetime TIMESTAMP,
  measurement_time VARCHAR(10),
  measurement_type_concept_id INT,
  operator_concept_id INT,
  value_as_number NUMERIC,
  value_as_concept_id INT,
  unit_concept_id INT,
  range_low NUMERIC,
  range_high NUMERIC,
  provider_id INT,
  visit_occurrence_id INT,
  visit_detail_id INT,
  measurement_source_value VARCHAR(50),
  measurement_source_concept_id INT,
  unit_source_value VARCHAR(50),
  unit_source_concept_id INT,
  value_source_value VARCHAR(50),
  measurement_event_id BIGINT,
  meas_event_field_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/measurement.csv' OVERWRITE INTO TABLE eunomia.measurement_temp;

CREATE OR REPLACE TABLE eunomia.measurement USING DELTA AS SELECT * from eunomia.measurement_temp where 1 <> 1;
INSERT INTO eunomia.measurement
SELECT 
  measurement_id, 
  person_id, 
  measurement_concept_id, 
  measurement_date, 
  measurement_datetime, 
  measurement_time, 
  measurement_type_concept_id, 
  nullif(operator_concept_id, '') as operator_concept_id, 
  value_as_number, 
  value_as_concept_id, 
  unit_concept_id, 
  range_low, 
  range_high, 
  nullif(provider_id, '') as provider_id, 
  nullif(visit_occurrence_id, '') as visit_occurrence_id, 
  nullif(visit_detail_id, '') as visit_detail_id, 
  nullif(measurement_source_value, '') as measurement_source_value, 
  nullif(measurement_source_concept_id, '') as measurement_source_concept_id, 
  nullif(unit_source_value, '') as unit_source_value, 
  nullif(unit_source_concept_id, '') as unit_source_concept_id, 
  nullif(value_source_value, '') as value_source_value, 
  measurement_event_id, 
  nullif(meas_event_field_concept_id, '') as meas_event_field_concept_id
FROM eunomia.measurement_temp WHERE upper(measurement_id) <> 'MEASUREMENT_ID';

DROP TABLE IF EXISTS eunomia.measurement_temp;

-----------------------------------------------
-- OBSERVATION
-----------------------------------------------

DROP TABLE IF EXISTS eunomia.observation_temp;

CREATE TABLE eunomia.observation_temp (
  observation_id INT,
  person_id INT,
  observation_concept_id INT,
  observation_date DATE,
  observation_datetime TIMESTAMP,
  observation_type_concept_id INT,
  value_as_number NUMERIC,
  value_as_string VARCHAR(60),
  value_as_concept_id INT,
  qualifier_concept_id INT,
  unit_concept_id INT,
  provider_id INT,
  visit_occurrence_id INT,
  visit_detail_id INT,
  observation_source_value VARCHAR(50),
  observation_source_concept_id INT,
  unit_source_value VARCHAR(50),
  qualifier_source_value VARCHAR(50),
  value_source_value VARCHAR(50),
  observation_event_id BIGINT,
  obs_event_field_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/observation.csv' OVERWRITE INTO TABLE eunomia.observation_temp;

CREATE OR REPLACE TABLE eunomia.observation USING DELTA AS SELECT * from eunomia.observation_temp where 1 <> 1;
INSERT INTO eunomia.observation
SELECT
  observation_id,
  person_id,
  observation_concept_id,
  observation_date,
  observation_datetime,
  observation_type_concept_id,
  value_as_number,
  nullif(value_as_string, '') as value_as_string,
  nullif(value_as_concept_id, '') as value_as_concept_id,
  nullif(qualifier_concept_id, '') as qualifier_concept_id,
  nullif(unit_concept_id, '') as unit_concept_id,
  nullif(provider_id, '') as provider_id,
  nullif(visit_occurrence_id, '') as visit_occurrence_id,
  nullif(visit_detail_id, '') as visit_detail_id,
  nullif(observation_source_value, '') as observation_source_value,
  nullif(observation_source_concept_id, '') as observation_source_concept_id,
  nullif(unit_source_value, '') as unit_source_value,
  nullif(qualifier_source_value, '') as qualifier_source_value,
  nullif(value_source_value, '') as value_source_value,
  nullif(observation_event_id, '') as observation_event_id,
  nullif(obs_event_field_concept_id, '') as obs_event_field_concept_id
FROM eunomia.observation_temp WHERE upper(observation_id) <> 'OBSERVATION_ID';

DROP TABLE IF EXISTS eunomia.observation_temp;

-----------------------------------------------
-- DEATH
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.death_temp;
CREATE TABLE eunomia.death_temp (
            person_id INT,
            death_date DATE,
            death_datetime TIMESTAMP,
            death_type_concept_id INT,
            cause_concept_id INT,
            cause_source_value VARCHAR(50),
            cause_source_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/death.csv' OVERWRITE INTO TABLE eunomia.death_temp;

CREATE OR REPLACE TABLE eunomia.death USING DELTA AS SELECT * from eunomia.death_temp where 1 <> 1;

INSERT INTO eunomia.death
SELECT
  person_id,
  death_date,
  death_datetime,
  nullif(death_type_concept_id, '') as death_type_concept_id,
  nullif(cause_concept_id, '') as cause_concept_id,
  nullif(cause_source_value, '') as cause_source_value,
  nullif(cause_source_concept_id, '') as cause_source_concept_id
FROM eunomia.death_temp WHERE person_id <> 'PERSON_ID';

DROP TABLE IF EXISTS eunomia.death_temp;

-----------------------------------------------
-- NOTE
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.note_temp;
CREATE TABLE eunomia.note_temp (
            note_id INT,
            person_id INT,
            note_date DATE,
            note_datetime TIMESTAMP,
            note_type_concept_id INT,
            note_class_concept_id INT,
            note_title VARCHAR(250),
            note_text STRING,
            encoding_concept_id INT,
            language_concept_id INT,
            provider_id INT,
            visit_occurrence_id INT,
            visit_detail_id INT,
            note_source_value VARCHAR(50),
            note_event_id BIGINT,
            note_event_field_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/note.csv' OVERWRITE INTO TABLE eunomia.note_temp;

CREATE OR REPLACE TABLE eunomia.note USING DELTA AS SELECT * from eunomia.note_temp where 1 <> 1;

INSERT INTO eunomia.note
SELECT
  note_id,
  person_id,
  note_date,
  note_datetime,
  note_type_concept_id,
  note_class_concept_id,
  nullif(note_title, '') as note_title,
  note_text,
  encoding_concept_id,
  language_concept_id,
  nullif(provider_id, '') as provider_id,
  nullif(visit_occurrence_id, '') as visit_occurrence_id,
  nullif(visit_detail_id, '') as visit_detail_id,
  nullif(note_source_value, '') as note_source_value,
  nullif(note_event_id, '') as note_event_id,
  nullif(note_event_field_concept_id, '') as note_event_field_concept_id
FROM eunomia.note_temp WHERE upper(note_id) <> 'NOTE_ID';

DROP TABLE IF EXISTS eunomia.note_temp;

-----------------------------------------------
-- NOTE_NLP
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.note_nlp_temp;

CREATE TABLE eunomia.note_nlp_temp (
            note_nlp_id INT,
            note_id INT,
            section_concept_id INT,
            snippet VARCHAR(250),
            offset VARCHAR(50),
            lexical_variant VARCHAR(250),
            note_nlp_concept_id INT,
            note_nlp_source_concept_id INT,
            nlp_system VARCHAR(250),
            nlp_date DATE,
            nlp_datetime TIMESTAMP,
            term_exists VARCHAR(1),
            term_temporal VARCHAR(50),
            term_modifiers VARCHAR(2000)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/note_nlp.csv' OVERWRITE INTO TABLE eunomia.note_nlp_temp;

CREATE OR REPLACE TABLE eunomia.note_nlp USING DELTA AS SELECT * from eunomia.note_nlp_temp where 1 <> 1;

INSERT INTO eunomia.note_nlp
SELECT 
  note_nlp_id, 
  note_id, 
  nullif(section_concept_id, '') as section_concept_id, 
  snippet, 
  "offset", 
  lexical_variant, 
  nullif(note_nlp_concept_id, '') as note_nlp_concept_id, 
  nullif(note_nlp_source_concept_id, '') as note_nlp_source_concept_id, 
  nlp_system, 
  nlp_date, 
  nlp_datetime, 
  term_exists, 
  term_temporal, 
  term_modifiers
FROM eunomia.note_nlp_temp WHERE upper(note_nlp_id) <> 'NOTE_NLP_ID';

DROP TABLE IF EXISTS eunomia.note_nlp_temp;

-----------------------------------------------
-- SPECIMEN
-----------------------------------------------

DROP TABLE IF EXISTS eunomia.specimen_temp;

CREATE TABLE eunomia.specimen_temp (
            specimen_id INT,
            person_id INT,
            specimen_concept_id INT,
            specimen_type_concept_id INT,
            specimen_date DATE,
            specimen_datetime TIMESTAMP,
            quantity NUMERIC,
            unit_concept_id INT,
            anatomic_site_concept_id INT,
            disease_status_concept_id INT,
            specimen_source_id VARCHAR(50),
            specimen_source_value VARCHAR(50),
            unit_source_value VARCHAR(50),
            anatomic_site_source_value VARCHAR(50),
            disease_status_source_value VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/specimen.csv' OVERWRITE INTO TABLE eunomia.specimen_temp;

CREATE OR REPLACE TABLE eunomia.specimen USING DELTA AS SELECT * from eunomia.specimen_temp where 1 <> 1;

INSERT INTO eunomia.specimen
SELECT 
  specimen_id, 
  person_id, 
  specimen_concept_id, 
  specimen_type_concept_id, 
  specimen_date, 
  specimen_datetime, 
  quantity, 
  nullif(unit_concept_id, '') as unit_concept_id, 
  nullif(anatomic_site_concept_id, '') as anatomic_site_concept_id, 
  nullif(disease_status_concept_id, '') as disease_status_concept_id, 
  nullif(specimen_source_id, '') as specimen_source_id, 
  nullif(specimen_source_value, '') as specimen_source_value, 
  nullif(unit_source_value, '') as unit_source_value, 
  nullif(anatomic_site_source_value, '') as anatomic_site_source_value, 
  nullif(disease_status_source_value, '') as disease_status_source_value
FROM eunomia.specimen_temp WHERE upper(specimen_id) <> 'SPECIMEN_ID';

DROP TABLE IF EXISTS eunomia.specimen_temp;

-----------------------------------------------
-- FACT_RELATIONSHIP
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.fact_relationship_temp;

CREATE TABLE eunomia.fact_relationship_temp (
            domain_concept_id_1 INT,
            fact_id_1 INT,
            domain_concept_id_2 INT,
            fact_id_2 INT,
            relationship_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/fact_relationship.csv' OVERWRITE INTO TABLE eunomia.fact_relationship_temp;

CREATE OR REPLACE TABLE eunomia.fact_relationship USING DELTA AS SELECT * from eunomia.fact_relationship_temp where 1 <> 1;

INSERT INTO eunomia.fact_relationship
SELECT 
  domain_concept_id_1, 
  fact_id_1, 
  domain_concept_id_2, 
  fact_id_2, 
  relationship_concept_id
FROM eunomia.fact_relationship_temp WHERE upper(domain_concept_id_1) <> 'DOMAIN_CONCEPT_ID_1';

DROP TABLE IF EXISTS eunomia.fact_relationship_temp;

-----------------------------------------------
-- LOCATION
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.location_temp;

CREATE TABLE eunomia.location_temp (
            location_id INT,
            address_1 VARCHAR(50),
            address_2 VARCHAR(50),
            city VARCHAR(50),
            state VARCHAR(2),
            zip VARCHAR(9),
            county VARCHAR(20),
            location_source_value VARCHAR(50),
            country_concept_id INT,
            country_source_value VARCHAR(80),
            latitude NUMERIC,
            longitude NUMERIC
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/location.csv' OVERWRITE INTO TABLE eunomia.location_temp;

CREATE OR REPLACE TABLE eunomia.location USING DELTA AS SELECT * from eunomia.location_temp where 1 <> 1;

INSERT INTO eunomia.location
SELECT 
  location_id, 
  nullif(address_1, '') as address_1, 
  nullif(address_2, '') as address_2, 
  nullif(city, '') as city, 
  nullif(state, '') as state, 
  nullif(zip, '') as zip, 
  nullif(county, '') as county, 
  nullif(location_source_value, '') as location_source_value, 
  nullif(country_concept_id, '') as country_concept_id, 
  nullif(country_source_value, '') as country_source_value, 
  nullif(latitude, '') as latitude, 
  nullif(longitude, '') as longitude
FROM eunomia.location_temp WHERE upper(location_id) <> 'LOCATION_ID';

DROP TABLE IF EXISTS eunomia.location_temp;

-----------------------------------------------
-- CARE_SITE
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.care_site_temp;

CREATE TABLE eunomia.care_site_temp (
            care_site_id INT,
            care_site_name VARCHAR(255),
            place_of_service_concept_id INT,
            location_id INT,
            care_site_source_value VARCHAR(50),
            place_of_service_source_value VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/care_site.csv' OVERWRITE INTO TABLE eunomia.care_site_temp;

CREATE OR REPLACE TABLE eunomia.care_site USING DELTA AS SELECT * from eunomia.care_site_temp where 1 <> 1;

INSERT INTO eunomia.care_site
SELECT 
  care_site_id, 
  nullif(care_site_name, '') as care_site_name, 
  nullif(place_of_service_concept_id, '') as place_of_service_concept_id, 
  nullif(location_id, '') as location_id, 
  nullif(care_site_source_value, '') as care_site_source_value, 
  nullif(place_of_service_source_value, '') as place_of_service_source_value
FROM eunomia.care_site_temp WHERE upper(care_site_id) <> 'CARE_SITE_ID';

DROP TABLE IF EXISTS eunomia.care_site_temp;

-----------------------------------------------
-- PROVIDER
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.provider_temp;

CREATE TABLE eunomia.provider_temp (
            provider_id INT,
            provider_name VARCHAR(255),
            npi VARCHAR(20),
            dea VARCHAR(20),
            specialty_concept_id INT,
            care_site_id INT,
            year_of_birth INT,
            gender_concept_id INT,
            provider_source_value VARCHAR(50),
            specialty_source_value VARCHAR(50),
            specialty_source_concept_id INT,
            gender_source_concept_id INT,
            gender_source_value VARCHAR(50)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/provider.csv' OVERWRITE INTO TABLE eunomia.provider_temp;

CREATE OR REPLACE TABLE eunomia.provider USING DELTA AS SELECT * from eunomia.provider_temp where 1 <> 1;

INSERT INTO eunomia.provider
SELECT 
  provider_id, 
  nullif(provider_name, '') as provider_name, 
  nullif(npi, '') as npi, 
  nullif(dea, '') as dea, 
  nullif(specialty_concept_id, '') as specialty_concept_id, 
  nullif(care_site_id, '') as care_site_id, 
  nullif(year_of_birth, '') as year_of_birth, 
  nullif(gender_concept_id, '') as gender_concept_id, 
  nullif(provider_source_value, '') as provider_source_value, 
  nullif(specialty_source_value, '') as specialty_source_value, 
  nullif(specialty_source_concept_id, '') as specialty_source_concept_id, 
  nullif(gender_source_value, '') as gender_source_value, 
  nullif(gender_source_concept_id, '') as gender_source_concept_id
FROM eunomia.provider_temp WHERE upper(provider_id) <> 'PROVIDER_ID';

DROP TABLE IF EXISTS eunomia.provider_temp;

-----------------------------------------------
-- PAYER_PLAN_PERIOD
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.payer_plan_period_temp;

CREATE TABLE eunomia.payer_plan_period_temp (
            payer_plan_period_id INT,
            person_id INT,
            payer_plan_period_start_date DATE,
            payer_plan_period_end_date DATE,
            payer_concept_id INT,
            payer_source_value VARCHAR(50),
            payer_source_concept_id INT,
            plan_concept_id INT,
            plan_source_value VARCHAR(50),
            plan_source_concept_id INT,
            sponsor_concept_id INT,
            sponsor_source_value VARCHAR(50),
            sponsor_source_concept_id INT,
            family_source_value VARCHAR(50),
            stop_reason_concept_id INT,
            stop_reason_source_value VARCHAR(50),
            stop_reason_source_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/payer_plan_period.csv' OVERWRITE INTO TABLE eunomia.payer_plan_period_temp;

CREATE OR REPLACE TABLE eunomia.payer_plan_period USING DELTA AS SELECT * from eunomia.payer_plan_period_temp where 1 <> 1;

INSERT INTO eunomia.payer_plan_period
SELECT 
  payer_plan_period_id, 
  person_id, 
  payer_plan_period_start_date, 
  payer_plan_period_end_date, 
  nullif(payer_concept_id, '') as payer_concept_id, 
  nullif(payer_source_value, '') as payer_source_value, 
  nullif(payer_source_concept_id, '') as payer_source_concept_id, 
  nullif(plan_concept_id, '') as plan_concept_id, 
  nullif(plan_source_value, '') as plan_source_value, 
  nullif(plan_source_concept_id, '') as plan_source_concept_id, 
  nullif(sponsor_concept_id, '') as sponsor_concept_id, 
  nullif(sponsor_source_value, '') as sponsor_source_value, 
  nullif(sponsor_source_concept_id, '') as sponsor_source_concept_id, 
  nullif(family_source_value, '') as family_source_value, 
  nullif(stop_reason_concept_id, '') as stop_reason_concept_id, 
  nullif(stop_reason_source_value, '') as stop_reason_source_value, 
  nullif(stop_reason_source_concept_id, '') as stop_reason_source_concept_id
FROM eunomia.payer_plan_period_temp WHERE upper(payer_plan_period_id) <> 'PAYER_PLAN_PERIOD_ID';

DROP TABLE IF EXISTS eunomia.payer_plan_period_temp;

-----------------------------------------------
-- COST
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.cost_temp;

CREATE TABLE eunomia.cost_temp (
            cost_id INT,
            cost_event_id INT,
            cost_domain_id VARCHAR(20),
            cost_type_concept_id INT,
            currency_concept_id INT,
            total_charge NUMERIC,
            total_cost NUMERIC,
            total_paid NUMERIC,
            paid_by_payer NUMERIC,
            paid_by_patient NUMERIC,
            paid_patient_copay NUMERIC,
            paid_patient_coinsurance NUMERIC,
            paid_patient_deductible NUMERIC,
            paid_by_primary NUMERIC,
            paid_ingredient_cost NUMERIC,
            paid_dispensing_fee NUMERIC,
            payer_plan_period_id INT,
            amount_allowed NUMERIC,
            revenue_code_concept_id INT,
            revenue_code_source_value VARCHAR(50),
            drg_concept_id INT,
            drg_source_value VARCHAR(3)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/cost.csv' OVERWRITE INTO TABLE eunomia.cost_temp;

CREATE OR REPLACE TABLE eunomia.cost USING DELTA AS SELECT * from eunomia.cost_temp where 1 <> 1;

INSERT INTO eunomia.cost
SELECT 
  cost_id, 
  cost_event_id, 
  cost_domain_id, 
  cost_type_concept_id, 
  nullif(currency_concept_id, '') as currency_concept_id, 
  nullif(total_charge, '') as total_charge, 
  nullif(total_cost, '') as total_cost, 
  nullif(total_paid, '') as total_paid, 
  nullif(paid_by_payer, '') as paid_by_payer, 
  nullif(paid_by_patient, '') as paid_by_patient, 
  nullif(paid_patient_copay, '') as paid_patient_copay, 
  nullif(paid_patient_coinsurance, '') as paid_patient_coinsurance, 
  nullif(paid_patient_deductible, '') as paid_patient_deductible, 
  nullif(paid_by_primary, '') as paid_by_primary, 
  nullif(paid_ingredient_cost, '') as paid_ingredient_cost, 
  nullif(paid_dispensing_fee, '') as paid_dispensing_fee, 
  nullif(payer_plan_period_id, '') as payer_plan_period_id, 
  nullif(amount_allowed, '') as amount_allowed, 
  nullif(revenue_code_concept_id, '') as revenue_code_concept_id, 
  nullif(revenue_code_source_value, '') as revenue_code_source_value, 
  nullif(drg_concept_id, '') as drg_concept_id, 
  nullif(drg_source_value, '') as drg_source_value
FROM eunomia.cost_temp WHERE upper(cost_id) <> 'COST_ID';

DROP TABLE IF EXISTS eunomia.cost_temp;

-----------------------------------------------
-- DRUG_ERA
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.drug_era_temp;

CREATE TABLE eunomia.drug_era_temp (
            drug_era_id INT,
            person_id INT,
            drug_concept_id INT,
            drug_era_start_date DATE,
            drug_era_end_date DATE,
            drug_exposure_count INT,
            gap_days INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/drug_era.csv' OVERWRITE INTO TABLE eunomia.drug_era_temp;

CREATE OR REPLACE TABLE eunomia.drug_era USING DELTA AS SELECT * from eunomia.drug_era_temp where 1 <> 1;

INSERT INTO eunomia.drug_era
SELECT 
  drug_era_id, 
  person_id, 
  drug_concept_id, 
  drug_era_start_date, 
  drug_era_end_date, 
  nullif(drug_exposure_count, '') as drug_exposure_count, 
  nullif(gap_days, '') as gap_days
FROM eunomia.drug_era_temp WHERE upper(drug_era_id) <> 'DRUG_ERA_ID';

DROP TABLE IF EXISTS eunomia.drug_era_temp;

-----------------------------------------------
-- DOSE_ERA
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.dose_era_temp;

CREATE TABLE eunomia.dose_era_temp (
            dose_era_id INT,
            person_id INT,
            drug_concept_id INT,
            unit_concept_id INT,
            dose_value NUMERIC,
            dose_era_start_date DATE,
            dose_era_end_date DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/dose_era.csv' OVERWRITE INTO TABLE eunomia.dose_era_temp;

CREATE OR REPLACE TABLE eunomia.dose_era USING DELTA AS SELECT * from eunomia.dose_era_temp where 1 <> 1;

INSERT INTO eunomia.dose_era
SELECT 
  dose_era_id, 
  person_id, 
  drug_concept_id, 
  unit_concept_id, 
  dose_value, 
  dose_era_start_date, 
  dose_era_end_date
FROM eunomia.dose_era_temp WHERE upper(dose_era_id) <> 'DOSE_ERA_ID';

DROP TABLE IF EXISTS eunomia.dose_era_temp;

-----------------------------------------------
-- CONDITION_ERA
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.condition_era_temp;

CREATE TABLE eunomia.condition_era_temp (
            condition_era_id INT,
            person_id INT,
            condition_concept_id INT,
            condition_era_start_date DATE,
            condition_era_end_date DATE,
            condition_occurrence_count INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/condition_era.csv' OVERWRITE INTO TABLE eunomia.condition_era_temp;

CREATE OR REPLACE TABLE eunomia.condition_era USING DELTA AS SELECT * from eunomia.condition_era_temp where 1 <> 1;

INSERT INTO eunomia.condition_era
SELECT 
  condition_era_id, 
  person_id, 
  condition_concept_id, 
  condition_era_start_date, 
  condition_era_end_date, 
  nullif(condition_occurrence_count, '') as condition_occurrence_count
FROM eunomia.condition_era_temp WHERE upper(condition_era_id) <> 'CONDITION_ERA_ID';

DROP TABLE IF EXISTS eunomia.condition_era_temp;

-----------------------------------------------
-- EPISODE
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.episode_temp;

CREATE TABLE eunomia.episode_temp (
            episode_id INT,
            person_id INT,
            episode_concept_id INT,
            episode_start_date DATE,
            episode_start_datetime TIMESTAMP,
            episode_end_date DATE,
            episode_end_datetime TIMESTAMP,
            episode_parent_id INT,
            episode_number INT,
            episode_object_concept_id INT,
            episode_type_concept_id INT,
            episode_source_value VARCHAR(50),
            episode_source_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

-- there is no episode.csv file in the demo data directory
--LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/episode.csv' OVERWRITE INTO TABLE eunomia.episode_temp;

CREATE OR REPLACE TABLE eunomia.episode USING DELTA AS SELECT * from eunomia.episode_temp where 1 <> 1;

INSERT INTO eunomia.episode
SELECT 
  episode_id, 
  person_id, 
  episode_concept_id, 
  episode_start_date, 
  nullif(episode_start_datetime, '') as episode_start_datetime, 
  nullif(episode_end_date, '') as episode_end_date, 
  nullif(episode_end_datetime, '') as episode_end_datetime, 
  nullif(episode_parent_id, '') as episode_parent_id, 
  nullif(episode_number, '') as episode_number, 
  episode_object_concept_id, 
  episode_type_concept_id, 
  nullif(episode_source_value, '') as episode_source_value, 
  nullif(episode_source_concept_id, '') as episode_source_concept_id
FROM eunomia.episode_temp WHERE upper(episode_id) <> 'EPISODE_ID';

DROP TABLE IF EXISTS eunomia.episode_temp;

-----------------------------------------------
-- EPISODE_EVENT
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.episode_event_temp;

CREATE TABLE eunomia.episode_event_temp (
            episode_id INT,
            event_id INT,
            episode_event_field_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

-- there is no episode_event.csv file in the demo data directory
--LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/episode_event.csv' OVERWRITE INTO TABLE eunomia.episode_event_temp;

CREATE OR REPLACE TABLE eunomia.episode_event USING DELTA AS SELECT * from eunomia.episode_event_temp where 1 <> 1;

INSERT INTO eunomia.episode_event
SELECT 
  episode_id, 
  event_id, 
  episode_event_field_concept_id
FROM eunomia.episode_event_temp WHERE upper(episode_id) <> 'EPISODE_ID';

DROP TABLE IF EXISTS eunomia.episode_event_temp;

-----------------------------------------------
-- METADATA
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.metadata_temp;

CREATE TABLE eunomia.metadata_temp (
            metadata_id INT,
            metadata_concept_id INT,
            metadata_type_concept_id INT,
            name VARCHAR(250),
            value_as_string VARCHAR(250),
            value_as_concept_id INT,
            value_as_number NUMERIC,
            metadata_date DATE,
            metadata_datetime TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

-- there is no metadata.csv file in the demo data directory
--LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/metadata.csv' OVERWRITE INTO TABLE eunomia.metadata_temp;

CREATE OR REPLACE TABLE eunomia.metadata USING DELTA AS SELECT * from eunomia.metadata_temp where 1 <> 1;

INSERT INTO eunomia.metadata
SELECT 
  metadata_id, 
  metadata_concept_id, 
  metadata_type_concept_id, 
  name, 
  nullif(value_as_string, '') as value_as_string, 
  nullif(value_as_concept_id, '') as value_as_concept_id, 
  nullif(value_as_number, '') as value_as_number, 
  nullif(metadata_date, '') as metadata_date, 
  nullif(metadata_datetime, '') as metadata_datetime
FROM eunomia.metadata_temp WHERE upper(metadata_id) <> 'METADATA_ID';

DROP TABLE IF EXISTS eunomia.metadata_temp;

-----------------------------------------------
-- CDM_SOURCE
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.cdm_source_temp;
CREATE TABLE eunomia.cdm_source_temp (
			cdm_source_name VARCHAR(255),
			cdm_source_abbreviation VARCHAR(25),
			cdm_holder VARCHAR(255),
			source_description STRING,
			source_documentation_reference VARCHAR(255),
			cdm_etl_reference VARCHAR(255),
			source_release_date DATE,
			cdm_release_date DATE,
			cdm_version VARCHAR(10),
			cdm_version_concept_id INT,
			vocabulary_version VARCHAR(20)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');
      
LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/cdm_source.csv' OVERWRITE INTO TABLE eunomia.cdm_source_temp;

CREATE OR REPLACE TABLE eunomia.cdm_source USING DELTA AS SELECT * from eunomia.cdm_source_temp where 1 <> 1;
INSERT INTO eunomia.cdm_source
SELECT
            cdm_source_name,
			cdm_source_abbreviation,
			cdm_holder,
			nullif(source_description, '') as source_description,
			nullif(source_documentation_reference, '') as source_documentation_reference,
			nullif(cdm_etl_reference, '') as cdm_etl_reference,
			source_release_date,
			cdm_release_date,
			nullif(cdm_version, '') as cdm_version,
			cdm_version_concept_id,
			vocabulary_version
FROM eunomia.cdm_source_temp WHERE upper(cdm_source_name) <> 'CDM_SOURCE_NAME';

DROP TABLE IF EXISTS eunomia.cdm_source_temp;

-----------------------------------------------
-- CONCEPT
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.concept_temp;
CREATE TABLE eunomia.concept_temp (
			concept_id INT,
			concept_name VARCHAR(255),
			domain_id VARCHAR(20),
			vocabulary_id VARCHAR(20),
			concept_class_id VARCHAR(20),
			standard_concept VARCHAR(1),
			concept_code VARCHAR(50),
			valid_start_date DATE,
			valid_end_date DATE,
			invalid_reason VARCHAR(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/concept.csv' OVERWRITE INTO TABLE eunomia.concept_temp;

CREATE OR REPLACE TABLE eunomia.concept USING DELTA AS SELECT * from eunomia.concept_temp where 1 <> 1;
INSERT INTO eunomia.concept
SELECT
            concept_id,
			concept_name,
			domain_id,
			vocabulary_id,
			concept_class_id,
			nullif(standard_concept, '') as standard_concept,
			concept_code,
			valid_start_date,
			valid_end_date,
			nullif(invalid_reason, '') as invalid_reason
FROM eunomia.concept_temp where upper(concept_id) <> 'CONCEPT_ID';

DROP TABLE IF EXISTS eunomia.concept_temp;

-----------------------------------------------
-- VOCABULARY
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.vocabulary_temp;

CREATE TABLE eunomia.vocabulary_temp (
            vocabulary_id VARCHAR(20),
            vocabulary_name VARCHAR(255),
            vocabulary_reference VARCHAR(255),
            vocabulary_version VARCHAR(255),
            vocabulary_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/vocabulary.csv' OVERWRITE INTO TABLE eunomia.vocabulary_temp;

CREATE OR REPLACE TABLE eunomia.vocabulary USING DELTA AS SELECT * from eunomia.vocabulary_temp where 1 <> 1;
INSERT INTO eunomia.vocabulary
SELECT
            vocabulary_id,
            vocabulary_name,
            nullif(vocabulary_reference, '') as vocabulary_reference,
            nullif(vocabulary_version, '') as vocabulary_version,
            vocabulary_concept_id
FROM eunomia.vocabulary_temp where upper(vocabulary_id) <> 'VOCABULARY_ID';

DROP TABLE IF EXISTS eunomia.vocabulary_temp;

-----------------------------------------------
-- DOMAIN
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.domain_temp;

CREATE TABLE eunomia.domain_temp (
            domain_id VARCHAR(20),
            domain_name VARCHAR(255),
            domain_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/domain.csv' OVERWRITE INTO TABLE eunomia.domain_temp;

CREATE OR REPLACE TABLE eunomia.domain USING DELTA AS SELECT * from eunomia.domain_temp where 1 <> 1;
INSERT INTO eunomia.domain
SELECT
            domain_id,
            domain_name,
            domain_concept_id
FROM eunomia.domain_temp where upper(domain_id) <> 'DOMAIN_ID';

DROP TABLE IF EXISTS eunomia.domain_temp;

-----------------------------------------------
--  CONCEPT_CLASS
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.concept_class_temp;

CREATE TABLE eunomia.concept_class_temp (
            concept_class_id VARCHAR(20),
            concept_class_name VARCHAR(255),
            concept_class_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/concept_class.csv' OVERWRITE INTO TABLE eunomia.concept_class_temp;

CREATE OR REPLACE TABLE eunomia.concept_class USING DELTA AS SELECT * from eunomia.concept_class_temp where 1 <> 1;
INSERT INTO eunomia.concept_class
SELECT
            concept_class_id,
            concept_class_name,
            concept_class_concept_id
FROM eunomia.concept_class_temp where upper(concept_class_id) <> 'CONCEPT_CLASS_ID';

DROP TABLE IF EXISTS eunomia.concept_class_temp;

-----------------------------------------------
-- CONCEPT_RELATIONSHIP
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.concept_relationship_temp;

CREATE TABLE eunomia.concept_relationship_temp (
            concept_id_1 INT,
            concept_id_2 INT,
            relationship_id VARCHAR(20),
            valid_start_date DATE,
            valid_end_date DATE,
            invalid_reason VARCHAR(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/concept_relationship.csv' OVERWRITE INTO TABLE eunomia.concept_relationship_temp;

CREATE OR REPLACE TABLE eunomia.concept_relationship USING DELTA AS SELECT * from eunomia.concept_relationship_temp where 1 <> 1;
INSERT INTO eunomia.concept_relationship
SELECT
            concept_id_1,
            concept_id_2,
            relationship_id,
            valid_start_date,
            valid_end_date,
            nullif(invalid_reason, '') as invalid_reason
FROM eunomia.concept_relationship_temp where upper(concept_id_1) <> 'CONCEPT_ID_1';

DROP TABLE IF EXISTS eunomia.concept_relationship_temp;

-----------------------------------------------
-- RELATIONSHIP
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.relationship_temp;

CREATE TABLE eunomia.relationship_temp (
            relationship_id VARCHAR(20),
            relationship_name VARCHAR(255),
            is_hierarchical VARCHAR(1),
            defines_ancestry VARCHAR(1),
            reverse_relationship_id VARCHAR(20),
            relationship_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/relationship.csv' OVERWRITE INTO TABLE eunomia.relationship_temp;

CREATE OR REPLACE TABLE eunomia.relationship USING DELTA AS SELECT * from eunomia.relationship_temp where 1 <> 1;
INSERT INTO eunomia.relationship
SELECT
            relationship_id,
            relationship_name,
            is_hierarchical,
            defines_ancestry,
            reverse_relationship_id,
            relationship_concept_id
FROM eunomia.relationship_temp where upper(relationship_id) <> 'RELATIONSHIP_ID';

DROP TABLE IF EXISTS eunomia.relationship_temp;

-----------------------------------------------
-- CONCEPT_SYNONYM
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.concept_synonym_temp;

CREATE TABLE eunomia.concept_synonym_temp (
            concept_id INT,
            concept_synonym_name VARCHAR(1000),
            language_concept_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/concept_synonym.csv' OVERWRITE INTO TABLE eunomia.concept_synonym_temp;

CREATE OR REPLACE TABLE eunomia.concept_synonym USING DELTA AS SELECT * from eunomia.concept_synonym_temp where 1 <> 1;
INSERT INTO eunomia.concept_synonym
SELECT
            concept_id,
            concept_synonym_name,
            language_concept_id
FROM eunomia.concept_synonym_temp where upper(concept_id) <> 'CONCEPT_ID';

DROP TABLE IF EXISTS eunomia.concept_synonym_temp;

-----------------------------------------------
-- CONCEPT_ANCESTOR
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.concept_ancestor_temp;

CREATE TABLE eunomia.concept_ancestor_temp (
            ancestor_concept_id INT,
            descendant_concept_id INT,
            min_levels_of_separation INT,
            max_levels_of_separation INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/concept_ancestor.csv' OVERWRITE INTO TABLE eunomia.concept_ancestor_temp;

CREATE OR REPLACE TABLE eunomia.concept_ancestor USING DELTA AS SELECT * from eunomia.concept_ancestor_temp where 1 <> 1;
INSERT INTO eunomia.concept_ancestor
SELECT
            ancestor_concept_id,
            descendant_concept_id,
            min_levels_of_separation,
            max_levels_of_separation
FROM eunomia.concept_ancestor_temp where upper(ancestor_concept_id) <> 'ANCESTOR_CONCEPT_ID';

DROP TABLE IF EXISTS eunomia.concept_ancestor_temp;

-----------------------------------------------
-- SOURCE_TO_CONCEPT_MAP
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.source_to_concept_map_temp;

CREATE TABLE eunomia.source_to_concept_map_temp (
            source_code VARCHAR(50),
            source_concept_id INT,
            source_vocabulary_id VARCHAR(20),
            source_code_description VARCHAR(255),
            target_concept_id INT,
            target_vocabulary_id VARCHAR(20),
            valid_start_date DATE,
            valid_end_date DATE,
            invalid_reason VARCHAR(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/source_to_concept_map.csv' OVERWRITE INTO TABLE eunomia.source_to_concept_map_temp;

CREATE OR REPLACE TABLE eunomia.source_to_concept_map USING DELTA AS SELECT * from eunomia.source_to_concept_map_temp where 1 <> 1;
INSERT INTO eunomia.source_to_concept_map
SELECT
            source_code,
            source_concept_id,
            source_vocabulary_id,
            source_code_description,
            target_concept_id,
            target_vocabulary_id,
            valid_start_date,
            valid_end_date,
            invalid_reason
FROM eunomia.source_to_concept_map_temp where upper(source_code) <> 'SOURCE_CODE';

DROP TABLE IF EXISTS eunomia.source_to_concept_map_temp;

-----------------------------------------------
-- DRUG_STRENGTH
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.drug_strength_temp;

CREATE TABLE eunomia.drug_strength_temp (
            drug_concept_id INT,
            ingredient_concept_id INT,
            amount_value NUMERIC,
            amount_unit_concept_id INT,
            numerator_value NUMERIC,
            numerator_unit_concept_id INT,
            denominator_value NUMERIC,
            denominator_unit_concept_id INT,
            box_size INT,
            valid_start_date DATE,
            valid_end_date DATE,
            invalid_reason VARCHAR(1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/drug_strength.csv' OVERWRITE INTO TABLE eunomia.drug_strength_temp;

CREATE OR REPLACE TABLE eunomia.drug_strength USING DELTA AS SELECT * from eunomia.drug_strength_temp where 1 <> 1;
INSERT INTO eunomia.drug_strength
SELECT
            drug_concept_id,
            ingredient_concept_id,
            amount_value,
            amount_unit_concept_id,
            numerator_value,
            numerator_unit_concept_id,
            denominator_value,
            denominator_unit_concept_id,
            box_size,
            valid_start_date,
            valid_end_date,
            invalid_reason
FROM eunomia.drug_strength_temp where upper(drug_concept_id) <> 'DRUG_CONCEPT_ID';

DROP TABLE IF EXISTS eunomia.drug_strength_temp;

-----------------------------------------------
-- COHORT
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.cohort_temp;

CREATE TABLE eunomia.cohort_temp (
            cohort_definition_id INT,
            subject_id INT,
            cohort_start_date DATE,
            cohort_end_date DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');

LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/cohort.csv' OVERWRITE INTO TABLE eunomia.cohort_temp;

CREATE OR REPLACE TABLE eunomia.cohort USING DELTA AS SELECT * from eunomia.cohort_temp where 1 <> 1;
INSERT INTO eunomia.cohort
SELECT
            cohort_definition_id,
            subject_id,
            cohort_start_date,
            cohort_end_date
FROM eunomia.cohort_temp where upper(cohort_definition_id) <> 'COHORT_DEFINITION_ID';

DROP TABLE IF EXISTS eunomia.cohort_temp;

-----------------------------------------------
-- COHORT_DEFINITION
-----------------------------------------------
DROP TABLE IF EXISTS eunomia.cohort_definition_temp;

CREATE TABLE eunomia.cohort_definition_temp (
            cohort_definition_id INT,
            cohort_definition_name VARCHAR(255),
            cohort_definition_description STRING,
            definition_type_concept_id INT,
            cohort_definition_syntax STRING,
            subject_concept_id INT,
            cohort_initiation_date DATE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
TBLPROPERTIES('dateFormat'='yyyy-MM-dd');


-- there is no cohort_definition.csv file in the demo data directory
--LOAD DATA LOCAL INPATH '/demo_cdm_csv_files/cohort_definition.csv' OVERWRITE INTO TABLE eunomia.cohort_definition_temp;

CREATE OR REPLACE TABLE eunomia.cohort_definition USING DELTA AS SELECT * from eunomia.cohort_definition_temp where 1 <> 1;
INSERT INTO eunomia.cohort_definition
SELECT
            cohort_definition_id,
            cohort_definition_name,
            cohort_definition_description,
            definition_type_concept_id,
            cohort_definition_syntax,
            subject_concept_id,
            cohort_initiation_date
FROM eunomia.cohort_definition_temp where upper(cohort_definition_id) <> 'COHORT_DEFINITION_ID';

DROP TABLE IF EXISTS eunomia.cohort_definition_temp;