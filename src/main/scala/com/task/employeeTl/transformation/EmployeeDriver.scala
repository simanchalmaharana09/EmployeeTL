package com.task.employeeTl.transformation

import com.task.employeeTl.util.Utility
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

    var employeeDF = Utility.readDfFromSource(spark, employeeDetailsPath).toDF("empId", "firstName", "lastName", "age")

    val departmentDF = Utility.readDfFromSource(spark, departmentDetailsPath).toDF("deptId", "deptName")

    var employeeFinanceDF = Utility.readDfFromSource(spark, employeeFinanceDetailsPath).toDF("empId", "ctc", "basic", "pf", "gratuity")

    val employeeDepartmentDF = Utility.readDfFromSource(spark, employeeDeptDetailsPath).toDF("empId", "deptId")

    // for each scenario we need age > 35 and >40. so filtering for >35
    employeeDF = EmployeeTrans.filterEmployeeByAge(employeeDF)

    // To make dataset smaller, applied both filter for 2 scenario - ctc > 30000 || gratuity < 800
    employeeFinanceDF = EmployeeTrans.filterEmpFinanceByCtcAndGrats(employeeFinanceDF)

    // emp age > 35 and (ctc > 30000 || gratuity < 800)
    val employeeWithFinanceDF = employeeDF
      .join(employeeFinanceDF, employeeDF("empId") === employeeFinanceDF("empId"))
      .drop("basic", "pf")
      .drop(employeeFinanceDF("empId")) // deleting one empId column, not from both DF

    // Caching joined employee and Finance data for further use ( 2nd scenario )
    employeeWithFinanceDF.cache()

    // filter emp for age > 40 & ctc > 30,000. But dropping gratuity column for final output
    val empAge40PlusCtc30kPlus = EmployeeTrans.filterForAge40PlusCtc30kPlus(employeeWithFinanceDF.drop("gratuity"))
    //empAge40PlusCtc30kPlus.show(10)
    // Persisting final result into output location
    Utility.writeDfIntoTarget(employeeFilteredResPath, empAge40PlusCtc30kPlus)

    ////---- Computation for dept with max emp with age > 35 & gratuity < 800
    // already age > 35 filter applied at top. filtering only for grat < 800
    val empAge35PlusGratuity800less = EmployeeTrans.filterEmpForGrat800Less(employeeWithFinanceDF)

    // broadcast join among filtered employee finance data and employees department
    val departmentForFilteredEmp = DepartmentTrans.getDeptForFilteredEmp(employeeDepartmentDF, empAge35PlusGratuity800less.select("empId"))

    // performing windowing operation for department and counting employee per dept
    val departmentWithEmpGrouped = DepartmentTrans.getDeptWithGroupedEmp(departmentForFilteredEmp)

    if (departmentWithEmpGrouped.first() != null) {
      // selecting deptId with max count
      val deptWithMaxCount = departmentWithEmpGrouped.select("deptId").first

      // getting department details for max count employee
      val resultDept = DepartmentTrans.getDeptDetailsForMaxCount(departmentDF, deptWithMaxCount)
      //resultDept.show(10)
      // Writing final result to hdfs
      Utility.writeDfIntoTarget(departmentMaxEmpResPath, resultDept)
    }
  }
}

