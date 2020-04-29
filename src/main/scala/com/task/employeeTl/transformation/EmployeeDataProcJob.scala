package com.task.employeeTl.transformation

import com.task.employeeTl.util.Utility
import com.task.employeeTl.util.Utility.EnvProperties
import org.apache.spark.sql.SparkSession

object EmployeeDataProcJob {
  def main(args: Array[String]): Unit = {
    val env: String = "dev" // may be args(0)
    transformEmployeeDetails(env)
  }

  def transformEmployeeDetails(env: String) {
    val spark = SparkSession.builder().appName("EmployeeDetails").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // Reading properties as per environment
    val props: EnvProperties = Utility.readProperties(env)

    var employeeDF = Utility.readDfFromSource(spark, props.employeeDetailsPath).toDF("empId", "firstName", "lastName", "age")

    val departmentDF = Utility.readDfFromSource(spark, props.departmentDetailsPath).toDF("deptId", "deptName")

    var employeeFinanceDF = Utility.readDfFromSource(spark, props.employeeFinanceDetailsPath).toDF("empId", "ctc", "basic", "pf", "gratuity")

    val employeeDepartmentDF = Utility.readDfFromSource(spark, props.employeeDeptDetailsPath).toDF("empId", "deptId")

    val employeeTrans: EmployeeTrans = new EmployeeTrans
    // for each scenario we need age > 35 and >40. so filtering for >35
    employeeDF = employeeTrans.filterEmployeeByAge(employeeDF)

    // To make dataset smaller, applied both filter for 2 scenario - ctc > 30000 || gratuity < 800
    employeeFinanceDF = employeeTrans.filterEmpFinanceByCtcAndGrats(employeeFinanceDF)

    // emp age > 35 and (ctc > 30000 || gratuity < 800)
    val employeeWithFinanceDF = employeeDF
      .join(employeeFinanceDF, employeeDF("empId") === employeeFinanceDF("empId"))
      .drop("basic", "pf")
      .drop(employeeFinanceDF("empId")) // deleting one empId column, not from both DF

    // Caching joined employee and Finance data for further use ( 2nd scenario )
    employeeWithFinanceDF.cache()

    // filter emp for age > 40 & ctc > 30,000. But dropping gratuity column for final output
    val empAge40PlusCtc30kPlus = employeeTrans.filterForAge40PlusCtc30kPlus(employeeWithFinanceDF.drop("gratuity"))

    // Persisting final result into output location
    Utility.writeDfIntoTarget(props.employeeFilteredResPath, empAge40PlusCtc30kPlus)

    ////---- Computation for dept with max emp with age > 35 & gratuity < 800
    // already age > 35 filter applied at top. filtering only for grat < 800
    val empAge35PlusGratuity800less = employeeTrans.filterEmpForGrat800Less(employeeWithFinanceDF)

    val departmentTrans: DepartmentTrans = new DepartmentTrans
    // broadcast join among filtered employee finance data and employees department
    val departmentForFilteredEmp = departmentTrans.getDeptForFilteredEmp(employeeDepartmentDF, empAge35PlusGratuity800less.select("empId"))

    // performing windowing operation for department and counting employee per dept
    val departmentWithEmpGrouped = departmentTrans.getDeptWithGroupedEmp(departmentForFilteredEmp)

    if (departmentWithEmpGrouped.first() != null) {
      // selecting deptId with max count
      val deptWithMaxCount = departmentWithEmpGrouped.select("deptId").first

      // getting department details for max count employee
      val resultDept = departmentTrans.getDeptDetailsForMaxCount(departmentDF, deptWithMaxCount)

      // Writing final result to hdfs
      Utility.writeDfIntoTarget(props.departmentMaxEmpResPath, resultDept)
    }
  }
}

