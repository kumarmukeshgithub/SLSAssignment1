package com.klm.sls.processor.etl

import org.apache.spark.sql.DataFrame

object WriteSLSIntoDelta {
  def writeJSonIntoDeltatable(data:DataFrame): Unit = {
    data.write
          .format("delta")
          .mode("overwrite")
          .save("/Users/mukeshbehera/Documents/FlightDeltalake")

  }
}
