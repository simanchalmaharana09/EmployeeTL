package com.task.employeeTl.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object utility {

  def readDfFromSource(spark: SparkSession, employeeDetailsPath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(employeeDetailsPath)
  }

  def writeDfIntoPath(employeeFilteredResPath: String, empAge40PlusCtc30kPlus: DataFrame) = {
    empAge40PlusCtc30kPlus.repartition(1).write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(employeeFilteredResPath)
  }
}
