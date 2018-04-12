/**
  * @author
  */

package edu.gatech.cse8803.main


import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{LabEvent, Prescription, Procedures, Patient}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors, Vector}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object Main {

  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val sqlContext = new SQLContext(sc)

    val (labEvent, prescription, procedure, patient) = loadRddRawData(sqlContext)


    //vitals, peripheral capillary oxygen saturation, Glasgow Coma Score, and age)

    /** feature construction with all features */
    val featureTuples = sc.union(FeatureConstruction.constructLabEventFeatureTuple(labEvent),
      FeatureConstruction.constructProceduresTuple(procedure)
       //FeatureConstruction.constructPrescriptionFeatureTuple(prescription),FeatureConstruction.constructPatientTuple(patient)
    )

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)



  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[LabEvent], RDD[Prescription], RDD[Procedures], RDD[Patient]) = {


    CSVUtils.loadCSVAsTable(sqlContext, "data/LABEVENTS.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/PRESCRIPTIONS.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/PROCEDURES_ICD.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/PATIENTS.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/ADMISSIONS.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/DIAGNOSES_ICD.csv")





    val labEvents: RDD[LabEvent] =  sqlContext.sql("SELECT P.SUBJECT_ID, L.ITEMID, 0 AS VALUENUM FROM PATIENTS P INNER JOIN ADMISSIONS A ON P.SUBJECT_ID = A.SUBJECT_ID LEFT JOIN DIAGNOSES_ICD D ON D.HADM_ID = A.HADM_ID INNER JOIN LABEVENTS L ON L.HADM_ID = A.HADM_ID WHERE D.ICD9_CODE IN ('78552','99591','99592') AND VALUENUM IS NOT NULL LIMIT 100 ".stripMargin).map(r => LabEvent(r(0).toString, r(1).toString, r(2).toString.toDouble))
    //val labEvents: RDD[LabEvent] =  sqlContext.sql("SELECT SUBJECT_ID, ITEMID, 0.0 as value FROM LABEVENTS LE  LIMIT 100 ".stripMargin).map(r => LabEvent(r(0).toString, r(1).toString, r(2).toString.toDouble))

    labEvents.cache()
    println(labEvents.count())

    val prescription: RDD[Prescription] =  sqlContext.sql("SELECT P.SUBJECT_ID, PR.DRUG, 0 AS DOSE_VAL_RX FROM PATIENTS P INNER JOIN ADMISSIONS A ON P.SUBJECT_ID = A.SUBJECT_ID LEFT JOIN DIAGNOSES_ICD D ON D.HADM_ID = A.HADM_ID INNER JOIN PRESCRIPTIONS PR ON PR.HADM_ID = A.HADM_ID WHERE D.ICD9_CODE IN ('78552','99591','99592') LIMIT 100 ".stripMargin).map(r => Prescription(r(0).toString, r(1).toString, r(2).toString.toDouble))
    //val prescription: RDD[Prescription] =  sqlContext.sql("SELECT SUBJECT_ID, DRUG, 1.0 AS THEVALUE FROM PRESCRIPTIONS  LIMIT 100".stripMargin).map(r => Prescription(r(0).toString, r(1).toString, r(2).toString.toDouble))
    prescription.cache()
    println(prescription.count())

    val procedure: RDD[Procedures] =  sqlContext.sql("SELECT P.SUBJECT_ID, PR.ICD9_CODE, 0 AS THECOUNT FROM PATIENTS P INNER JOIN ADMISSIONS A ON P.SUBJECT_ID = A.SUBJECT_ID LEFT JOIN DIAGNOSES_ICD D ON D.HADM_ID = A.HADM_ID INNER JOIN PROCEDURES_ICD PR ON PR.HADM_ID = A.HADM_ID WHERE D.ICD9_CODE IN ('78552','99591','99592') LIMIT 100 ".stripMargin).map(r => Procedures(r(0).toString, r(1).toString, r(2).toString.toInt))
    //val procedure: RDD[Procedures] =  sqlContext.sql("SELECT SUBJECT_ID, ICD9_CODE, 2 as THECOUNT FROM PROCEDURES_ICD  LIMIT 100".stripMargin).map(r => Procedures(r(0).toString, r(1).toString, r(2).toString.toInt))
    procedure.cache()
    println(procedure.count())

    val patient: RDD[Patient] =  sqlContext.sql("SELECT SUBJECT_ID, 35 as age, GENDER, 1 as genderIndicator, DOB, DOD FROM PATIENTS LIMIT 100".stripMargin).map(r => Patient(r(0).toString, r(1).toString.toInt, r(2).toString.toLowerCase, r(3).toString.toInt, r(4).toString, r(5).toString))
    patient.cache()
    println(patient.count())

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    patient.map(x => if (x.gender=="M" ) x.genderIndicator == 1 else x.genderIndicator == 0)
    //patient.map(x => x = )
    (labEvents, prescription, procedure, patient)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")
}
