package com.task.employeeTl.util

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utility {

  def readDfFromSource(spark: SparkSession, sourcePath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .load(sourcePath)
  }

  def writeDfIntoTarget(targetPath: String, resDf: DataFrame) = {
    resDf.repartition(1).write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(targetPath)
  }

  class EnvProperties(val employeeDetailsPath: String, val employeeFinanceDetailsPath: String, val employeeDeptDetailsPath: String, val departmentDetailsPath: String, val employeeFilteredResPath: String, val departmentMaxEmpResPath: String)

  def readProperties(ENV: String): EnvProperties = {

    val envPros = ConfigFactory.load().getConfig(ENV) // need to be declared as constant variable

    val employeeDetailsPath = envPros.getString(ConstAttr.EMPLOYEE_DETAILS_PATH)
    val employeeFinanceDetailsPath = envPros.getString(ConstAttr.EMPLOYEE_FINANCE_DETAILS_PATH)
    val employeeDeptDetailsPath = envPros.getString(ConstAttr.EMPLOYEE_DEPT_DETAILS_PATH)
    val departmentDetailsPath = envPros.getString(ConstAttr.DEPARTMENT_DETAILS_PATH)
    val employeeFilteredResPath = envPros.getString(ConstAttr.EMPLOYEE_FILTERED_RES_PATH)
    val departmentMaxEmpResPath = envPros.getString(ConstAttr.DEPARTMENT_MAX_EMP_RES_PATH)

    new EnvProperties(employeeDetailsPath, employeeFinanceDetailsPath, employeeDeptDetailsPath, departmentDetailsPath, employeeFilteredResPath, departmentMaxEmpResPath)
  }
}
