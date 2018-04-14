--COUNTS
SELECT COUNT(*) FROM PRESCRIPTIONS;
SELECT COUNT(*) FROM ADMISSIONS;
SELECT COUNT(*) FROM LABEVENTS;
SELECT COUNT(*) FROM PATIENTS;
SELECT COUNT(*) FROM PROCEDURES_ICD;


--***************************************  PATIENTS

--CALCULATE INDEX DATE FOR PATIENTS (LAST CHARTEVENT FOR NON SEPSIS PATIENTS, 30 DAYS FROM DOD FOR EXPIRED)
UPDATE P SET P.INDEX_DATE = C.INDEX_DATE FROM PATIENTS P, (
SELECT MAX(CHARTTIME) AS INDEX_DATE, CE.SUBJECT_ID  FROM CHARTEVENTS CE INNER JOIN PATIENTS P ON P.SUBJECT_ID = CE.SUBJECT_ID WHERE P.EXPIRE_FLAG = 0 GROUP BY CE.SUBJECT_ID 
) AS C WHERE P.SUBJECT_ID = C.SUBJECT_ID AND P.EXPIRE_FLAG = 0

UPDATE PATIENTS SET INDEX_DATE =DATEADD(DAY, -30, DOD) WHERE EXPIRE_FLAG = 1 --expired = true

UPDATE P  SET P.INDEX_DATE =  MAX(CE.CHARTTIME) FROM PATIENTS P INNER JOIN CHARTEVENTS CE ON P._SUBJECT_ID_ = CE._SUBJECT_ID_ GROUP BY P.SUBJECT_ID WHERE _EXPIRE_FLAG_ =  0  


SELECT TOP 10 DATEADD(DAY, -30, _DOD_)  FROM PATIENTS

SELECT TOP 10 * FROM PATIENTS

--CALCULATE PATIENT AGE
		
UPDATE  P SET P.FIRST_ADMISSIONTIME = C.FIRST_ADMITTIME FROM PATIENTS P, (
SELECT 
    P.SUBJECT_ID, P.DOB, P.GENDER, 
    MIN (A.ADMITTIME) AS FIRST_ADMITTIME
FROM PATIENTS P
INNER JOIN ADMISSIONS A
ON P.SUBJECT_ID = A.SUBJECT_ID
GROUP BY P.SUBJECT_ID, P.DOB, P.GENDER, A.HADM_ID
) C WHERE P.SUBJECT_ID = C.SUBJECT_ID



UPDATE P SET P.AGE_GROUP = C.AGE_GROUP FROM PATIENTS P, (

WITH first_admission_time AS
(
  SELECT
      p.subject_id, p.dob, p.gender, MIN (a.admittime) AS first_admittime
     , MIN( ROUND( DATEDIFF(dd,DOB,a.ADMITTIME) / 365.242,2) ) AS first_admit_age
  FROM patients p
  INNER JOIN admissions a
  ON p.subject_id = a.subject_id
  GROUP BY p.subject_id, p.dob, p.gender
  
)
SELECT
    subject_id, dob, gender
    , FIRST_ADMITTIME, first_admit_age
    , CASE
        -- all ages > 89 in the database were replaced with 300
        WHEN first_admit_age > 89
            then '>89'
        WHEN first_admit_age >= 14
            THEN 'adult'
        WHEN first_admit_age <= 1
            THEN 'neonate'
        ELSE 'middle'
        END AS age_group
FROM first_admission_time
ORDER BY subject_id


UPDATE P SET P.AGE_GROUP = C.AGE_GROUP FROM PATIENTS P, PATIENTS_AGE C WHERE P.SUBJECT_ID = C.SUBJECT_ID


-- SET GENDER_INDICATOR
UPDATE PATIENTS SET GENDER_INDICATOR = CASE
	WHEN GENDER = 'M' THEN '1'
	WHEN GENDER = 'F' THEN '0'
END

SELECT COUNT(*) FROM PATIENTS WHERE GENDER_INDICATOR IS NULL

--REMOVE NEONATES
DELETE FROM PATIENTS WHERE AGE_GROUP = 'NEONATE' OR AGE_GROUP IS NULL
SELECT ROW_ID,SUBJECT_ID,GENDER,DOB,DOD,DOD_HOSP,DOD_SSN,EXPIRE_FLAG,FIRST_ADMISSIONTIME,AGE_GROUP,INDEX_DATE,TRIM(GENDER_INDICATOR) FROM PATIENTS

--OUTPUT
SELECT * FROM PATIENTS

--**************************************************PRESCRIPTIONS
--SELECT MAXIMUM PER UNIT FOR NORMALIZATION
SELECT C.MAXIMUM, C.FORM_UNIT_DISP INTO PRESCRIPTIONMAX FROM (SELECT MAX(FORM_VAL_DISP) AS MAXIMUM,FORM_UNIT_DISP FROM PRESCRIPTIONS WHERE ISNUMERIC(FORM_VAL_DISP)=1
 GROUP BY FORM_UNIT_DISP) C

SELECT TOP 10 * FROM PRESCRIPTIONMAX



SELECT TOP 10 * FROM PRESCRIPTIONS


--CREATE NUMERIC FEATURE VALUES

UPDATE PRESCRIPTIONS SET FEATUREVALUE = FORM_VAL_DISP WHERE ISNUMERIC(FORM_VAL_DISP)=1
UPDATE PRESCRIPTIONS SET FEATUREVALUE = SUBSTRING(FEATUREVALUE,CHARINDEX('.',FEATUREVALUE,0),3) WHERE ISNUMERIC(FORM_VAL_DISP)=1
UPDATE PRESCRIPTIONS SET FEATUREVALUE = NULL WHERE FEATUREVALUE = '.'


--NORMALIZE
UPDATE P SET P.FEATUREVALUE = CAST(P.FEATUREVALUE AS DECIMAL(38,7)) / CAST(M.MAXIMUM AS DECIMAL(38,7)) FROM PRESCRIPTIONS P, PRESCRIPTIONMAX M 
WHERE P.FORM_UNIT_DISP = M.FORM_UNIT_DISP 
AND ISNUMERIC(P.FEATUREVALUE)=1 AND ISNUMERIC(M.MAXIMUM) = 1
AND M.FORM_UNIT_DISP IS NOT NULL 
AND M.MAXIMUM IS NOT NULL
AND FEATUREVALUE IS NOT NULL
AND CAST(M.MAXIMUM AS DECIMAL(38,7)) > 0


--REMOVE RECORDS WITHOUT VALUES
DELETE FROM PRESCRIPTIONS WHERE FEATUREVALUE IS NULL

--OUTPUT
SELECT TOP 1 ROW_ID,SUBJECT_ID, HADM_ID, DRUG, FEATUREVALUE FROM PRESCRIPTIONS




UPDATE PRESCRIPTIONS SET FEATUREVALUE = 0.2027 WHERE ROW_ID = 708449

--************************************ PROCEDURES
SELECT TOP 1 * FROM PROCEDURES_ICD

SELECT SUBJECT_ID, ICD9_CODE, COUNT(1) AS FEATURECOUNT INTO PROCEDURES_WITH_FEATURES  FROM PROCEDURES_ICD GROUP BY SUBJECT_ID, ICD9_CODE


SELECT P.SUBJECT_ID, PR.ICD9_CODE, 0 AS FEATURECOUNT FROM PATIENTS_WITH_FEATURES P 
	INNER JOIN ADMISSIONS A ON P.SUBJECT_ID = A.SUBJECT_ID LEFT JOIN DIAGNOSES_ICD D ON D.HADM_ID = A.HADM_ID 
	INNER JOIN PROCEDURES_ICD PR ON PR.HADM_ID = A.HADM_ID WHERE D.ICD9_CODE IN ('78552','99591','99592')


--RANDOM QUERIES
SELECT  ISNUMERIC(FORM_VAL_DISP), * FROM PRESCRIPTIONS WHERE ISNUMERIC(FEATUREVALUE)=1 AND FEATUREVALUE IS NOT NULL


SELECT TOP 10 ISNUMERIC(FORM_VAL_DISP), * FROM PRESCRIPTIONS WHERE ISNUMERIC(FORM_VAL_DISP)=1

SELECT FEATUREVALUE,MAXIMUM,ROW_ID
--, CAST(P.FEATUREVALUE AS DECIMAL(38,7)) / CAST(M.MAXIMUM AS DECIMAL(38,7)) 
FROM PRESCRIPTIONS P, PRESCRIPTIONMAX M  WHERE P.FORM_UNIT_DISP = M.FORM_UNIT_DISP 
AND ISNUMERIC(P.FEATUREVALUE)=1 AND ISNUMERIC(M.MAXIMUM) = 1
AND M.FORM_UNIT_DISP IS NOT NULL 
AND M.MAXIMUM IS NOT NULL
AND FEATUREVALUE IS NOT NULL
AND CAST(M.MAXIMUM AS DECIMAL(38,7)) > 0
AND FEATUREVALUE ='.'
AND M.FORM_UNIT_DISP IN (

'CAP'
) ORDER BY ROW_ID DESC



SELECT * FROM PRESCRIPTIONS P, PRESCRIPTIONMAX M WHERE  M.FORM_UNIT_DISP IS NOT NULL 
AND M.MAXIMUM IS NOT NULL
AND FEATUREVALUE IS NOT NULL
AND CAST(M.MAXIMUM AS DECIMAL(38,7)) > 0
AND M.FORM_UNIT_DISP IN (
'UNIT')
AND ROW_ID = 708449
AND SUBECT_ID = 26169
	
	
SELECT 
	CAST( SUBSTRING(FORM_VAL_DISP,CHARINDEX('-',FORM_VAL_DISP,0) + 1, LEN(FORM_VAL_DISP) ) AS decimal(20,20))
	-
	CAST(SUBSTRING(FORM_VAL_DISP,0,CHARINDEX('-',FORM_VAL_DISP,0)) AS decimal(20,20)) 
	AS FEATUREVALUE
FROM PRESCRIPTIONS WHERE ISNUMERIC(FORM_VAL_DISP) <> 1 AND FORM_VAL_DISP IS NOT NULL AND CHARINDEX('-',FORM_VAL_DISP) > 0  AND FORM_VAL_DISP NOT IN ('0.25-1ml','0.25-1 SYR','2.5-25-1','10-40ml')

	
	SELECT DISTINCT  FROM PRESCRIPTIONS WHERE FEATUREVALUE IS NOT NULL


	3920532
	4156450

	SELECT CHARINDEX('-',FORM_VAL_DISP,0) FROM PRESCRIPTIONS
	SELECT * FROM PRESCRIPTIONS P,  (
	SELECT  ROW_ID,SUBSTRING(FORM_VAL_DISP,CHARINDEX('-',FORM_VAL_DISP,0) + 1, LEN(FORM_VAL_DISP) )  AS FEATUREVALUE FROM PRESCRIPTIONS WHERE ISNUMERIC(FORM_VAL_DISP) <> 1
	)  C WHERE ISNUMERIC(C.FEATUREVALUE) <> 1 AND FORM_VAL_DISP IS NOT NULL AND CHARINDEX('-',FORM_VAL_DISP) > 0  AND FORM_VAL_DISP NOT IN ('0.25-1ml','0.25-1 SYR','2.5-25-1','10-40ml') AND P.ROW_ID = C.ROW_ID

	SELECT FORM_VAL_DISP, SUBSTRING(FORM_VAL_DISP,0,CHARINDEX('-',FORM_VAL_DISP,0)) FROM PRESCRIPTIONS WHERE ISNUMERIC(FORM_VAL_DISP) <> 1



EXEC sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO

EXEC sp_configure 'xp_cmdshell',1
GO
RECONFIGURE
GO

DECLARE @OutputFile NVARCHAR(100) ,    @FilePath NVARCHAR(100) ,    @bcpCommand NVARCHAR(1000)
 
SET @bcpCommand = 'bcp "SELECT * FROM PRESCRIPTIONS " queryout '
SET @FilePath = 'C:\Users\rbroderson\'
SET @OutputFile = 'FileName1.txt'
SET @bcpCommand = @bcpCommand + @FilePath + @OutputFile + ' -c -t, -T -S'+ @@servername
exec master..xp_cmdshell @bcpCommand

exec master..xp_fixeddrives
