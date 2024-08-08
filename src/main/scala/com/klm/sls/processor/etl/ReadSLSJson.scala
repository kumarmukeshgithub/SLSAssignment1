package com.klm.sls.processor.etl

import com.klm.sls.processor.schema.Root
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Encoders}

object ReadSLSJson{
  val schema1 = Encoders.product[Root].schema
  def readDataset(spark: SparkSession,filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .schema(schema1)
      .json(filePath)
  }
}