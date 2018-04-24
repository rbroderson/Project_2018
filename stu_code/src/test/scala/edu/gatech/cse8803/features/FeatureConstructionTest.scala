package edu.gatech.cse8803.features

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import edu.gatech.cse8803.model.LabEvent
import edu.gatech.cse8803.model.Prescription
import edu.gatech.cse8803.model.Procedures
import edu.gatech.cse8803.model.Patient

import org.apache.spark.mllib.linalg.Vectors

class FeatureConstructionTest extends FlatSpec with BeforeAndAfter with Matchers {

  var sparkContext: SparkContext = _

  before {
    val config = new SparkConf().setAppName("Test FeatureConstruction").setMaster("local")
    sparkContext = new SparkContext(config)
  }

  after {
    sparkContext.stop()
  }

  "constructLabEventFeatureTuple" should "aggregate one event" in {
    val labs = sparkContext.parallelize(Seq(
        new LabEvent("patient1", "A",1.0)));
    val actual = FeatureConstruction.constructLabEventFeatureTuple(labs).collect()
    val expected = Array(
        (("patient1", "A"), 1.0))
    actual should be (expected)
  }

  "constructLabEventFeatureTuple" should "aggregate two different events" in {
    val labs = sparkContext.parallelize(Seq(
        new LabEvent("patient1", "A", 1.0),
        new LabEvent("patient1", "B", 1.0)));
    val actual =  FeatureConstruction.constructLabEventFeatureTuple(labs).collectAsMap()
    val expected = Map(
        (("patient1", "A"), 1.0),
        (("patient1", "B"), 1.0))
    actual should be (expected)
  }

  "constructLabEventFeatureTuple" should "aggregate two same events" in {
    val labs = sparkContext.parallelize(Seq(
      new LabEvent("patient1", "A", 1.0),
      new LabEvent("patient1", "A", 1.0)));
    val actual = FeatureConstruction.constructLabEventFeatureTuple(labs).collect()
    val expected = Array(
        (("patient1", "A"), 2.0))
    actual should be (expected)
  }

  "constructLabEventFeatureTuple" should "aggregate three events with duplication" in {
    val labs = sparkContext.parallelize(Seq(
        new LabEvent("patient1",  "code1", 1.0),
        new LabEvent("patient1", "code1", 1.0),
        new LabEvent("patient1",  "code2", 1.0)));
    val actual = FeatureConstruction.constructLabEventFeatureTuple(labs).collectAsMap()
    val expected = Map(
        (("patient1", "code1"), 2.0),
        (("patient1", "code2"), 1.0))
    actual should be (expected)
  }




  "constructPrescriptionFeatureTuple" should "aggregate one event" in {
    val presrips = sparkContext.parallelize(Seq(
      new Prescription("patient1", "A",1.0)));
    val actual = FeatureConstruction.constructPrescriptionFeatureTuple(presrips).collect()
    val expected = Array(
      (("patient1", "A"), 1.0))
    actual should be (expected)
  }

  "constructPrescriptionFeatureTuple" should "aggregate two different events" in {
    val presrips = sparkContext.parallelize(Seq(
      new Prescription("patient1", "A", 1.0),
      new Prescription("patient1", "B", 1.0)));
    val actual =  FeatureConstruction.constructPrescriptionFeatureTuple(presrips).collectAsMap()
    val expected = Map(
      (("patient1", "A"), 1.0),
      (("patient1", "B"), 1.0))
    actual should be (expected)
  }

  "constructPrescriptionFeatureTuple" should "aggregate two same events" in {
    val presrips = sparkContext.parallelize(Seq(
      new Prescription("patient1", "A", 1.0),
      new Prescription("patient1", "A", 1.0)));
    val actual = FeatureConstruction.constructPrescriptionFeatureTuple(presrips).collect()
    val expected = Array(
      (("patient1", "A"), 2.0))
    actual should be (expected)
  }

  "constructPrescriptionFeatureTuple" should "aggregate three events with duplication" in {
    val presrips = sparkContext.parallelize(Seq(
      new Prescription("patient1",  "code1", 1.0),
      new Prescription("patient1", "code1", 1.0),
      new Prescription("patient1",  "code2", 1.0)));
    val actual = FeatureConstruction.constructPrescriptionFeatureTuple(presrips).collectAsMap()
    val expected = Map(
      (("patient1", "code1"), 2.0),
      (("patient1", "code2"), 1.0))
    actual should be (expected)
  }





  "constructProceduresFeatureTuple" should "aggregate one event" in {
    val procs = sparkContext.parallelize(Seq(
      new Procedures("patient1", "A",1)));
    val actual = FeatureConstruction.constructProceduresTuple(procs).collect()
    val expected = Array(
      (("patient1", "A"), 1))
    actual should be (expected)
  }

  "constructProceduresFeatureTuple" should "aggregate two different events" in {
    val procs = sparkContext.parallelize(Seq(
      new Procedures("patient1", "A", 1),
      new Procedures("patient1", "B", 1)));
    val actual =  FeatureConstruction.constructProceduresTuple(procs).collectAsMap()
    val expected = Map(
      (("patient1", "A"), 1),
      (("patient1", "B"), 1))
    actual should be (expected)
  }

  "constructProceduresFeatureTuple" should "aggregate two same events" in {
    val procs = sparkContext.parallelize(Seq(
      new Procedures("patient1", "A", 1),
      new Procedures("patient1", "A", 1)));
    val actual = FeatureConstruction.constructProceduresTuple(procs).collect()
    val expected = Array(
      (("patient1", "A"), 2.0))
    actual should be (expected)
  }

  "constructProceduresFeatureTuple" should "aggregate three events with duplication" in {
    val procs = sparkContext.parallelize(Seq(
      new Procedures("patient1",  "code1", 1),
      new Procedures("patient1", "code1", 1),
      new Procedures("patient1",  "code2", 1)));
    val actual = FeatureConstruction.constructProceduresTuple(procs).collectAsMap()
    val expected = Map(
      (("patient1", "code1"), 2.0),
      (("patient1", "code2"), 1.0))
    actual should be (expected)
  }




  "constructPatientsFeatureTuple" should "aggregate one event" in {
    val pats = sparkContext.parallelize(Seq(
      new Patient("patient1", "M", 1,1, 1)));
    val actual = FeatureConstruction.constructPatientTuple(pats).collect()
    val expected = Array(
      (("patient1", "M"), 1))
    actual should be (expected)
  }



}
