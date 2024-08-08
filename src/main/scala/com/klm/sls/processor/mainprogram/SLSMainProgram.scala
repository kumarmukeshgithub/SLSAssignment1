package com.klm.sls.processor.mainprogram

import com.klm.sls.processor.etl.FlattenSLSJson.SLSFlattenedJson
import com.klm.sls.processor.etl.PrintSLSJson.showSLSFlattenedData
import com.klm.sls.processor.etl.ReadSLSJson.readDataset
import com.klm.sls.processor.etl.WriteSLSIntoDelta.writeJSonIntoDeltatable
import com.klm.sls.processor.etl.WriteSLSIntoSql.writeSLSIntoSql
import org.apache.spark.sql.SparkSession


object SLSMainProgram {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Read Nested JSON Files")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()


    try {

      if (args.length < 1) {
        throw new IllegalArgumentException("Please provide the path to the dataset SLS Data file as the first argument.")
      }

      val filePathForDataSet = args(0)

      val rawDataset=readDataset(spark,filePathForDataSet)
      val flattenedDataset=SLSFlattenedJson(rawDataset)
      showSLSFlattenedData(flattenedDataset)
      //writeJSonIntoDeltatable(flattenedDataset)
      writeSLSIntoSql(flattenedDataset)
    }
    finally{
      spark.stop()
    }
  }
}
