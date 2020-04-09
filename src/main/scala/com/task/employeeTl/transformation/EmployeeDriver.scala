package com.task.employeeTl.transformation

import com.task.employeeTl.util.utility
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

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

    var employeeDF = utility.readDfFromSource(spark, employeeDetailsPath).toDF("empId", "firstName", "lastName", "age")

    val departmentDF = utility.readDfFromSource(spark, departmentDetailsPath).toDF("deptId", "deptName")

    var employeeFinanceDF = utility.readDfFromSource(spark, employeeFinanceDetailsPath).toDF("empId", "ctc", "basic", "pf", "gratuity")

    val employeeDepartmentDF = utility.readDfFromSource(spark, employeeDeptDetailsPath).toDF("empId", "deptId")

    // for each scenario we need age > 35 and >40. so filtering for >35
    employeeDF = EmployeeTrans.getEmployeeAge35Plus(employeeDF)

    // To make dataset smaller, applied both filter for 2 scenario - ctc > 30000 || gratuity < 800
    employeeFinanceDF = EmployeeTrans.getEmployeeFinanceDF(employeeFinanceDF)

    // emp with age > 40 & ctc > 30,000
    val employeeCtcDF = EmployeeTrans.getEmpDfAge40PlusCtc30kPlus(employeeDF, employeeFinanceDF)

    // Selecting only required columns for result
    val empAge40PlusCtc30kPlus = EmployeeTrans.selectDfCols(employeeDF, employeeCtcDF)

    // Persisting final result into output location
    utility.writeDfIntoPath(employeeFilteredResPath, empAge40PlusCtc30kPlus)

    // dept with max emp with age > 35 & gratuity < 800
    // already age > 35 filter applied at top
    val empAge35PlusGratuity800less = DepartmentTrans.getEmpAge35PlusGrat800Less(employeeDF, employeeFinanceDF)

    // broadcast join among filtered employee finance data and employees department
    val departmentWithEmpFiltered = DepartmentTrans.getDeptForFilteredEmp(employeeDF, employeeDepartmentDF, empAge35PlusGratuity800less)

    // performing windowing operation for department and counting employee per dept
    val departmentWithEmpGrouped = DepartmentTrans.getDeptWithGroupedEmp(departmentWithEmpFiltered)

    if (departmentWithEmpGrouped.first() != null) {
      // getting deptId with max count
      val deptWithMaxCount = DepartmentTrans.getDeptIdWithMaxCount(departmentWithEmpGrouped)

      // getting department details for max count employee
      val resultDept = DepartmentTrans.getDeptDetailsForMaxCount(departmentDF, deptWithMaxCount)

      // Writing final result to hdfs
      utility.writeDfIntoPath(departmentMaxEmpResPath, resultDept)
    }
  }
}

