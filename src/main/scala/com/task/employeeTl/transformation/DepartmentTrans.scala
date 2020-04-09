package com.task.employeeTl.transformation

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{broadcast, desc}

object DepartmentTrans {

  def getDeptDetailsForMaxCount(departmentDF: DataFrame, deptWithMaxCount: Row) = {
    departmentDF.filter(departmentDF("deptId") === deptWithMaxCount.get(0))
  }

  def getDeptIdWithMaxCount(departmentWithEmpGrouped: Dataset[Row]): Row = {
    departmentWithEmpGrouped.select("deptId").first
  }

  def getDeptWithGroupedEmp(departmentWithEmpFiltered: DataFrame) = {
    val windowByDepId = Window.partitionBy("deptId")
    departmentWithEmpFiltered
      .withColumn("count", org.apache.spark.sql.functions.count("empId").over(windowByDepId))
      .orderBy(desc("count"))
      .limit(1)
  }

  def getDeptForFilteredEmp(employeeDF: DataFrame, employeeDepartmentDF: DataFrame, empAge35PlusGratuity800less: DataFrame): DataFrame = {
    employeeDepartmentDF
      .join(broadcast(empAge35PlusGratuity800less.select(employeeDF("empId"))), "empId")
      .select(employeeDepartmentDF("*"))
  }
}
