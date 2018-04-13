/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

import org.apache.spark.sql.types.DateType

case class LabEvent(patientID: String, itemId: String, value: Double)

case class Prescription(patientID: String, drug: String, value: Double)

//case class Diagnoses(patientID: String, date: Date, code: String)

case class Procedures(patientID: String, code: String, count: Int)

case class Patient(patientID: String, age: Int, gender: String, genderIndicator: Int, dob: String, dod: String, expiredFlag: Int)

