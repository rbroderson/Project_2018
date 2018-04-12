/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{LabEvent, Prescription, Procedures, Patient}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from labEvents with SUM aggregation,
   * @param labEvent RDD of labEvents
   * @return RDD of feature tuples
   */
  def constructLabEventFeatureTuple(labEvent: RDD[LabEvent]): RDD[FeatureTuple] = {

    val grpPatientsTotal = labEvent.map(x=>((x.patientID, x.itemId),x.value) )

    val grpPatientSum = grpPatientsTotal.groupByKey().map((x=> (x._1, (x._2.sum))))


    grpPatientSum
  }
 // list.map(x => if (x % 2 == 0) x * 2 else x / 2)
  /**
   * Aggregate feature tuples from prescription with SUM aggregation,
   * @param prescription RDD of prescription
   * @return RDD of feature tuple
   */
  def constructPrescriptionFeatureTuple(prescription: RDD[Prescription]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val grpPatientsTotal = prescription.map(x=>((x.patientID, x.drug),x.value) )

    val grpPatientSum = grpPatientsTotal.groupByKey().map((x=> (x._1, (x._2.sum))))

    grpPatientSum
  }

  /**
   * Aggregate feature tuples from procedures, using COUNT aggregation
   * @param procedures RDD of procedures
   * @return RDD of feature tuples
   */
  def constructProceduresTuple(procedures: RDD[Procedures]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val grpPatientsTotal = procedures.map(x=>((x.patientID, x.code),x.count) )

    val grpPatientAvg = grpPatientsTotal.groupByKey().map((x=> (x._1, (x._2.sum).toDouble)))


    grpPatientAvg
  }

  /**
    * Aggregate feature tuples from patient, using COUNT aggregation
    * @param patient RDD of patient
    * @return RDD of feature tuples
    */
  def constructPatientTuple(patient: RDD[Patient]): RDD[FeatureTuple] = {
    /**
      * TODO implement your own code here and remove existing
      * placeholder code
      */
    val grpPatientsTotal = patient.map(x=>((x.patientID, x.gender),x.genderIndicator.toDouble) )

    //val grpPatientAvg = grpPatientsTotal.groupByKey().map((x=> (x._1, (x._2.sum).toDouble)))


    grpPatientsTotal
  }
  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map*/

    /** transform input feature */

    /**
     * Functions maybe helpful:
     *    collect
     *    groupByKey
     */

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val featuresbyPatID = feature.map(x=> (x._1._1,x._1._2,x._2))

    val grpPatients = featuresbyPatID.groupBy(x=>x._1).map(x=>x._2)


    val patientTargetAndFeatures = grpPatients.map{events =>

    val targetEvent = events.find(x=>x._1 != "").get
    val target = targetEvent._1

    val filteredFeatureEvents = events

    // aggregate events into features
    val features = filteredFeatureEvents.
      groupBy(x=>x._2).
      map{case(eventId, grpEvents) =>
        // user event id as feature name
        val featureName = eventId
        // event value sum as feature value
        val featureValue = grpEvents.map(x=>x._3).sum

        (featureName, featureValue)
      }

      println("target")
      target.take(5).foreach(println)
      println("features")
      features.take(5).foreach(println)

    (target, features)
  }


      val patients = patientTargetAndFeatures.map(x=>(x._1)).distinct()

/*

 */

    // assign a unique integer id to feature name
    val featureMap = patientTargetAndFeatures. // RDD[(target, Map[feature-name, feature-value])]
      flatMap(x=>x._2.keys). // get all feature names
      distinct. // remove duplication
      collect. // collect to driver program
      zipWithIndex. // assign an integer id
      toMap // convert to Map[feature-name, feature-id]


    // broadcast feature map from driver to all workers
    val scFeatureMap = sc.broadcast(featureMap)
    val finalSamples = patientTargetAndFeatures.map {case(target, features) =>
      val numFeature = scFeatureMap.value.size

      val indexedFeatures = features.
        toList.
        // map feature name to id to get List[(feature-id, feature-value)]
        map{case(featureName, featureValue) => (scFeatureMap.value(featureName), featureValue)}

      val featureVector = Vectors.sparse(numFeature, indexedFeatures)
      val labeledPoint = (target, featureVector)
      labeledPoint


    }

    finalSamples.take(10).foreach(println)

    finalSamples

  }

}


