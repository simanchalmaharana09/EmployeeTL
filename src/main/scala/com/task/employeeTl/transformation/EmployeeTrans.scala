package com.task.employeeTl.transformation

import org.apache.spark.sql.{DataFrame, Dataset, Row}

object EmployeeTrans {
  def getEmployeeAge35Plus(employeeDF: DataFrame) = {
    employeeDF
      .filter(employeeDF("age") > 35)
  }

  def getEmployeeFinanceDF(employeeFinanceDF: DataFrame): Dataset[Row] = {
    employeeFinanceDF
      .filter(employeeFinanceDF("ctc") > 30000 || employeeFinanceDF("gratuity") < 800)
  }

  def getEmpDfAge40PlusCtc30kPlus(employeeDF: DataFrame, employeeFinanceDF: DataFrame) = {
    employeeDF
      .filter(employeeDF("age") > 40)
      .join(employeeFinanceDF
        .filter(employeeFinanceDF("ctc") > 30000), employeeDF("empId") === employeeFinanceDF("empId"))
  }

  def selectDfCols(employeeDF: DataFrame, employeeCtcDF: DataFrame) = {
    employeeCtcDF.select(employeeDF("*"), employeeCtcDF("ctc"))
  }

}
