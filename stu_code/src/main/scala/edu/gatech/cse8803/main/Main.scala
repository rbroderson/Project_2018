/**
  * @author
  */

package edu.gatech.cse8803.main


import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{LabEvent, Prescription, Procedures, Patient}
import org.apache.spark

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}

import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans, StreamingKMeans}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors, Vector}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import scala.io.Source
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils


object Main {

  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext("local","local[*]")
    val sqlContext = new SQLContext(sc)


    print(sc.version)


    val (labEvent, prescription, procedure, patient) = loadRddRawData(sqlContext)


    //vitals, peripheral capillary oxygen saturation, Glasgow Coma Score, and age)

    /** feature construction with all features */
    val featureTuples = sc.union(FeatureConstruction.constructLabEventFeatureTuple(labEvent),
      FeatureConstruction.constructProceduresTuple(procedure), FeatureConstruction.constructPrescriptionFeatureTuple(prescription),FeatureConstruction.constructPatientTuple(patient)
    )

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)

    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray)))})

    val positiveSepsis = patient.filter(x => x.expiredFlag == 1).keyBy(x => x.patientID).join(features.keyBy(x => x._1)).map(x => LabeledPoint(1.0, x._2._2._2))
    val negativeSepsis = patient.filter(x => x.expiredFlag == 0).keyBy(x => x.patientID).join(features.keyBy(x => x._1)).map(x => LabeledPoint(0.0, x._2._2._2))


    MLUtils.saveAsLibSVMFile(positiveSepsis.union(negativeSepsis), "data/svm")
    val data = MLUtils.loadLibSVMFile(sc,"data/svm")


    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(training)


    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
    val recall = metrics.recall
    println("Recall = " + recall)
    val F1 = metrics.fMeasure
    println("fMeasure = " + F1)
    //model.save(sc, "data/model")
    //val sameModel = LogisticRegressionModel.load(sc, "data/model")

    val metricsROC = new BinaryClassificationMetrics(predictionAndLabels)
    val auROC = metricsROC.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Split the data into training and test sets (30% held out for testing)
    val splitsRF = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splitsRF(0), splitsRF(1))


    // Train a RandomForest model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32

    val modelRF = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
    println("Test Mean Squared Error = " + testMSE)

    println("Learned regression forest model:\n" + model.toString())

    // Save and load model
   // model.save(sc, "myModelPath")
    //val sameModel = RandomForestModel.load(sc, "myModelPath")



  }





  def loadRddRawData(sqlContext: SQLContext): (RDD[LabEvent], RDD[Prescription], RDD[Procedures], RDD[Patient]) = {


    CSVUtils.loadCSVAsTable(sqlContext, "data/LABEVENTS.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/PRESCRIPTIONS_WITH_FEATURES.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/PROCEDURES_ICD.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/PATIENTS_WITH_FEATURES.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/ADMISSIONS.csv")

    CSVUtils.loadCSVAsTable(sqlContext, "data/DIAGNOSES_ICD.csv")




    //Add FEATUREVALUE to data file
    val labEvents: RDD[LabEvent] =  sqlContext.sql("SELECT P.SUBJECT_ID, L.ITEMID, 0 AS FEATUREVALUE FROM PATIENTS_WITH_FEATURES P INNER JOIN ADMISSIONS A ON P.SUBJECT_ID = A.SUBJECT_ID LEFT JOIN DIAGNOSES_ICD D ON D.HADM_ID = A.HADM_ID INNER JOIN LABEVENTS L ON L.HADM_ID = A.HADM_ID WHERE D.ICD9_CODE IN ('78552','99591','99592') AND VALUENUM IS NOT NULL ".stripMargin).map(r => LabEvent(r(0).toString, r(1).toString, r(2).toString.toDouble))
    //val labEvents: RDD[LabEvent] =  sqlContext.sql("SELECT SUBJECT_ID, ITEMID, 0.0 as value FROM LABEVENTS LE  LIMIT 100 ".stripMargin).map(r => LabEvent(r(0).toString, r(1).toString, r(2).toString.toDouble))

    labEvents.cache()
    println(labEvents.count())

    //Add FEATUREVALUE to data file
    val prescription: RDD[Prescription] =  sqlContext.sql("SELECT P.SUBJECT_ID, PR.DRUG, 0  AS FEATUREVALUE FROM PATIENTS_WITH_FEATURES P INNER JOIN ADMISSIONS A ON P.SUBJECT_ID = A.SUBJECT_ID LEFT JOIN DIAGNOSES_ICD D ON D.HADM_ID = A.HADM_ID INNER JOIN PRESCRIPTIONS_WITH_FEATURES PR ON PR.HADM_ID = A.HADM_ID WHERE D.ICD9_CODE IN ('78552','99591','99592') ".stripMargin).map(r => Prescription(r(0).toString, r(1).toString, r(2).toString.toDouble))
    //val prescription: RDD[Prescription] =  sqlContext.sql("SELECT SUBJECT_ID, DRUG, 1.0 AS THEVALUE FROM PRESCRIPTIONS  LIMIT 100".stripMargin).map(r => Prescription(r(0).toString, r(1).toString, r(2).toString.toDouble))
    prescription.cache()
    println(prescription.count())

    //Add FEATURECOUNT to data file
    val procedure: RDD[Procedures] =  sqlContext.sql("SELECT PR.SUBJECT_ID, PR.ICD9_CODE, COUNT(1) AS FEATURECOUNT FROM PATIENTS_WITH_FEATURES P INNER JOIN ADMISSIONS A ON P.SUBJECT_ID = A.SUBJECT_ID LEFT JOIN DIAGNOSES_ICD D ON D.HADM_ID = A.HADM_ID INNER JOIN PROCEDURES_ICD PR ON PR.HADM_ID = A.HADM_ID WHERE D.ICD9_CODE IN ('78552','99591','99592') GROUP BY PR.SUBJECT_ID, PR.ICD9_CODE ".stripMargin).map(r => Procedures(r(0).toString, r(1).toString, r(2).toString.toInt))
    //val procedure: RDD[Procedures] =  sqlContext.sql("SELECT SUBJECT_ID, ICD9_CODE, 2 as THECOUNT FROM PROCEDURES_ICD  LIMIT 100".stripMargin).map(r => Procedures(r(0).toString, r(1).toString, r(2).toString.toInt))
    procedure.cache()
    println(procedure.count())

    val patient: RDD[Patient] =  sqlContext.sql("SELECT SUBJECT_ID, AGE_GROUP, GENDER, GENDER_INDICATOR, DOB, DOD, EXPIRE_FLAG FROM PATIENTS_WITH_FEATURES ".stripMargin).map(r => Patient(r(0).toString, r(1).toString, r(2).toString.toLowerCase, r(3).toString.toInt, r(4).toString, r(5).toString, r(6).toString.toInt))
    patient.cache()
    println(patient.count())

    (labEvents, prescription, procedure, patient)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")
}
