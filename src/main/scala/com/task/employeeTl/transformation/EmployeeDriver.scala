package com.task.employeeTl.transformation

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, desc}

object EmployeeDriver {
  def main(args: Array[String]): Unit = {
    transformEmployeeDetails()
  }

  def transformEmployeeDetails() {
    val spark = SparkSession.builder().appName("EmployeeDetails").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val envPros = ConfigFactory.load().getConfig("dev") // need to be declared as constant variable

    val employeeDetailsPath = envPros.getString("employee_details_path")
    val employeeFinanceDetailsPath = envPros.getString("employee_finance_details_path")
    val employeeDeptDetailsPath = envPros.getString("employee_dept_details_path")
    val departmentDetailsPath = envPros.getString("department_details_path")
    val employeeFilteredResPath = envPros.getString("employee_filtered_res_path")
    val departmentMaxEmpResPath = envPros.getString("department_max_emp_res_path")

    var employeeDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(employeeDetailsPath).toDF("empId", "firstName", "lastName", "age")

    val departmentDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(departmentDetailsPath).toDF("deptId", "deptName")

    var employeeFinanceDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(employeeFinanceDetailsPath).toDF("empId", "ctc", "basic", "pf", "gratuity")

    val employeeDepartmentDF = spark.read
      .format("csv")
      .option("header", "true")
      .load(employeeDeptDetailsPath).toDF("empId", "deptId")

    // for each scenario we need age > 35 and >40. so filtering for >35
    employeeDF = employeeDF
      .filter(employeeDF("age") > 35)

    // To make dataset smaller, applied both filter for 2 scenario
    employeeFinanceDF = employeeFinanceDF
      .filter(employeeFinanceDF("ctc") > 30000 || employeeFinanceDF("gratuity") < 800)

    // emp with age > 40 & ctc > 30,000
    val employeeCtcDF = employeeDF
      .filter(employeeDF("age") > 40)
      .join(employeeFinanceDF
        .filter(employeeFinanceDF("ctc") > 30000), employeeDF("empId") === employeeFinanceDF("empId"))

    val empAge40PlusCtc30kPlus = employeeCtcDF.select(employeeDF("*"), employeeCtcDF("ctc"))

    empAge40PlusCtc30kPlus.repartition(1).write
      .mode("overwrite")
      .format("csv")
      .option("header", "true")
      .save(employeeFilteredResPath)

    // dept with max emp with age > 35 & gratuity < 800
    // already age > 35 filter applied at top
    val empAge35PlusGratuity800less = employeeDF
      .join(employeeFinanceDF
        .filter(employeeFinanceDF("gratuity") < 800), employeeDF("empId") === employeeFinanceDF("empId"))

    // broadcast join among filtered employee finance data and employees department
    val departmentWithEmpFiltered = employeeDepartmentDF
      .join(broadcast(empAge35PlusGratuity800less.select(employeeDF("empId"))), "empId")
      .select(employeeDepartmentDF("*"))

    // performing windowing operation for department and counting employee per dept
    val windowByDepId = Window.partitionBy("deptId")
    val departmentWithEmpGrouped = departmentWithEmpFiltered
      .withColumn("count", org.apache.spark.sql.functions.count("empId").over(windowByDepId))
      .orderBy(desc("count"))
      .limit(1)

    if (departmentWithEmpGrouped.first() != null) {
      // getting deptId with max count
      val deptWithMaxCount = departmentWithEmpGrouped.select("deptId").first

      // getting department details for max count employee
      val resultDept = departmentDF.filter(departmentDF("deptId") === deptWithMaxCount.get(0))

      resultDept.repartition(1)
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(departmentMaxEmpResPath)
    }
  }
}

