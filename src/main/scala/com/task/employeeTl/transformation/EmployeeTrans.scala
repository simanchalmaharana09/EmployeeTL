package com.task.employeeTl.transformation

import org.apache.spark.sql.{DataFrame, Dataset, Row}

class EmployeeTrans {
  def filterEmployeeByAge(employeeDF: DataFrame) = {
    employeeDF
      .filter(employeeDF("age") > 35)
  }

  def filterEmpFinanceByCtcAndGrats(employeeFinanceDF: DataFrame): Dataset[Row] = {
    employeeFinanceDF
      .filter(employeeFinanceDF("ctc") > 30000 || employeeFinanceDF("gratuity") < 800)
  }

  def filterForAge40PlusCtc30kPlus(employeeWithFinanceDF: DataFrame) = {
    employeeWithFinanceDF
      .filter(employeeWithFinanceDF("age") > 40 &&
        employeeWithFinanceDF("ctc") > 30000)
  }

  def filterEmpForGrat800Less(employeeWithFinanceDF: DataFrame): DataFrame = {
    employeeWithFinanceDF
      .filter(employeeWithFinanceDF("gratuity") < 800)
  }
}
