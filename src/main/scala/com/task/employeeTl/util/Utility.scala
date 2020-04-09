package com.task.employeeTl.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utility {

  def readDfFromSource(spark: SparkSession, sourcePath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(sourcePath)
  }

  def writeDfIntoPath(targetPath: String, resDf: DataFrame) = {
    resDf.repartition(1).write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(targetPath)
  }
}
