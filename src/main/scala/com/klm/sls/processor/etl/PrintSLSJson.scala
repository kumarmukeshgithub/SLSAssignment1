package com.klm.sls.processor.etl
import org.apache.spark.sql.DataFrame


object PrintSLSJson {
  def showSLSFlattenedData(dataFrame: DataFrame): Unit = {

    dataFrame.show()

  }
}
