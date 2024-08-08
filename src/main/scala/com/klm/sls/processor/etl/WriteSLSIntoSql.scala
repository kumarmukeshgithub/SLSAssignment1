package com.klm.sls.processor.etl

import org.apache.spark.sql.DataFrame


object WriteSLSIntoSql {
  def writeSLSIntoSql(data:DataFrame): Unit = {
    val url = "jdbc:mysql://localhost:3306/demo"
    val table = "flight_data"
    val properties = new java.util.Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "myname23")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val connection = java.sql.DriverManager.getConnection(url, properties)

    data.write.mode("append").jdbc(url, table, properties)

    connection.close()


  }

}
