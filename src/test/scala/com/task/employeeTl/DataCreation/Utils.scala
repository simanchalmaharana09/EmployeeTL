package com.task.employeeTl.DataCreation

import com.typesafe.config.ConfigFactory

import scala.io.Source

case class Employee(emp_id: Int, first_name: String, last_name: String, age: Int)

case class EmployeeFinance(emp_id: Int, ctc: Double, basic: Double, pf: Double, gratuity: Double)

case class EmployeeDept(emp_id: Int, dept_id: Int)

case class Department(dept_id: Int, dept_name: String)

case class EnvProperties(val surNameMap: Map[Int, String], val employeeDetailsPath: String, val employeeFinanceDetailsPath: String, val employeeDeptDetailsPath: String, val departmentDetailsPath: String, val ageRangeStart: Int, val ageRangeEnd: Int, val ctcRangeStart: Int, val ctcRangeEnd: Int, val noOfEmployee: Int)

class Utils {

  def populateProperties(env: String): EnvProperties = {
    val surNameMap: Map[Int, String] = Map(1 -> "Sahoo", 2 -> "Pradhan", 3 -> "Rath", 4 -> "Swain", 5 -> "Reddy", 6 -> "Trivedi", 7 -> "Kumar", 8 -> "Singh")

    val envPros = ConfigFactory.load().getConfig(env) // need to be declared as constant variable
    val employeeDetailsPath = envPros.getString("employee_details_path")
    val employeeFinanceDetailsPath = envPros.getString("employee_finance_details_path")
    val employeeDeptDetailsPath = envPros.getString("employee_dept_details_path")
    val departmentDetailsPath = envPros.getString("department_details_path")
    //println(employeeDetailsPath)
    val ageRangeStart = envPros.getString("age_range_start").toInt // need to check for valid value
    val ageRangeEnd = envPros.getString("age_range_end").toInt
    val ctcRangeStart = envPros.getString("ctc_range_start").toInt
    val ctcRangeEnd = envPros.getString("ctc_range_end").toInt
    val noOfEmployee = envPros.getString("no_of_employee").toInt

    EnvProperties(surNameMap, employeeDetailsPath, employeeFinanceDetailsPath, employeeDeptDetailsPath, departmentDetailsPath, ageRangeStart, ageRangeEnd, ctcRangeStart, ctcRangeEnd, noOfEmployee)
  }

  def prepareDeptList(deptDataFilePath: String): List[Int] = {
    val department = Source.fromFile(deptDataFilePath)
    val deptIdList: List[Int] = department.getLines().toList.drop(1).map(line => {
      val arr = line.split(",")
      if (arr != null && arr(0) != null)
        arr(0).toInt
    }).map(_.toString.toInt)
    deptIdList
  }
}
