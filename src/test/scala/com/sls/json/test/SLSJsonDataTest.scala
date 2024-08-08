package com.sls.json.test

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}

class SLSJsonDataTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("RecordCountTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val basePath = "/Users/mukeshbehera/Downloads/RAW/CorporateManagement_Support/CorporateFinance/transactionaldata/PRA/SLST/SLSVALID/2024/07"

  val jsonFilePath = s"$basePath/*/*"

  val df= spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .json(jsonFilePath)

  test("Record count should be 18914") {

    val recordCount = df.count()

    assert(recordCount == 18914, s"Expected 18914 records, but found $recordCount")
  }
  test("DataFrame should contain the 'timestamp' column") {

    val hasGenreColumn = df.columns.contains("timestamp")

    assert(hasGenreColumn, "Expected DataFrame to contain column 'timestamp'")
  }
}
