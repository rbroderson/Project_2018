-- ***************************************************************************
-- Loading Data:
-- create external table mapping for tables

-- IMPORTANT NOTES:
-- You need to put tables under hdfs directories
-- '/input/admissions/ADMISSIONS.csv'
-- 
-- To do this, run the following commands for events.csv, 
-- 1. sudo su - hdfs
-- 2. hdfs dfs -mkdir -p /input/admissions
-- 3. hdfs dfs -chown -R root /input
-- 4. exit 
-- 5. hdfs dfs -put /path-to-admissions.csv /input/admissions/

-- ***************************************************************************
-- create admissions table

DROP TABLE IF EXISTS ADMISSIONS;
CREATE EXTERNAL TABLE ADMISSIONS (
  row_id STRING,
  subject_id STRING,
  hadm_id STRING,
  admittime DATE,
  dischtime DATE,
  deathtime DATE,
  admission_type STRING,
  admission_location STRING,
  discharge_location STRING,
  insurance STRING,
  language STRING,
  religion STRING,
  marital_status STRING,
  ethnicity STRING,
  edregtime DATE,
  edouttime DATE,
  diagnosis STRING,
  hospital_expire_flag INT,
  has_chartevents_data INT

  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/admissions';

SELECT COUNT(1) FROM ADMISSIONS;

-- create PATIENTS table

DROP TABLE IF EXISTS PATIENTS;
CREATE EXTERNAL TABLE PATIENTS (
  row_id STRING,
  subject_id STRING,
  gender STRING,
  dob DATE,
  dod DATE,
  dod_hosp DATE,
  dod_ssn DATE,
  expire_flag INT

  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/patient';

SELECT COUNT(1) FROM PATIENTS;


-- create Prescriptions table

DROP TABLE IF EXISTS PRESCRIPTIONS;
CREATE EXTERNAL TABLE PRESCRIPTIONS (
  row_id STRING,
  subject_id STRING,
  hadm_id STRING,
  icustay_id STRING,
  startdate DATE,
  enddate DATE,
  drug_type STRING,
  drug STRING,
  drug_name_poe STRING,
  drug_name_generic STRING,
  formulary_drug_cd STRING,
  gsn STRING,
  ndc STRING,
  prod_strength STRING,
  dose_val_rx INT,
  dose_unit_rx STRING,
  form_val_disp INT,
  form_unit_disp STRING,
  route STRING


  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/prescription';

SELECT COUNT(1) FROM PRESCRIPTIONS;


-- create DIAGNOSES_ICD table

DROP TABLE IF EXISTS DIAGNOSES_ICD;
CREATE EXTERNAL TABLE DIAGNOSES_ICD (
  row_id STRING,
  subject_id STRING,
  hadm_id STRING,
  seq_num INT,
  icd9_code STRING

  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/diagnose';

SELECT COUNT(1) FROM DIAGNOSES_ICD;

-- create PROCEDURES table

DROP TABLE IF EXISTS PROCEDURES_ICD9;
CREATE EXTERNAL TABLE PROCEDURES_ICD9 (
  row_id STRING,
  subject_id STRING,
  hadm_id STRING,
  seq_num INT,
  icd9_code STRING

  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/procedure';

SELECT COUNT(1) FROM PROCEDURES_ICD9;


-- create LABEVENTS table

DROP TABLE IF EXISTS LABEVENTS;
CREATE EXTERNAL TABLE LABEVENTS (
  row_id STRING,
  subject_id STRING,
  hadm_id STRING,
  itemid INT,
  charttime DATE,
  value INT,
  valuenum DOUBLE,
  valueuom STRING,
  flag STRING
  

  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/labevent';

SELECT COUNT(1) FROM LABEVENTS;



