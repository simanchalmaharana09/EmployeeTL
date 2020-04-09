package com.task.employeeTl.testdataprep

import com.gingersoftware.csv.ObjectCSV
import com.typesafe.config.ConfigFactory
import scala.io.Source
import scala.util.Random

case class Employee(emp_id: Int, first_name: String, last_name: String, age: Int)

case class EmployeeFinance(emp_id: Int, ctc: Double, basic: Double, pf: Double, gratuity: Double)

case class EmployeeDept(emp_id: Int, dept_id: Int)

case class Department(dept_id: Int, dept_name: String)

object CreateEmployeeData {
  def main(args: Array[String]): Unit = {
    val surName: Map[Int, String] = Map(1 -> "Sahoo", 2 -> "Pradhan", 3 -> "Rath", 4 -> "Swain", 5 -> "Reddy", 6 -> "Trivedi", 7 -> "Kumar", 8 -> "Singh")

    val envPros = ConfigFactory.load().getConfig("dev") // need to be declared as constant variable
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

    var employeeList = List[Employee]()
    var employeeFinanceList = List[EmployeeFinance]()
    var employeeDeptList = List[EmployeeDept]()

    val department = Source.fromFile(departmentDetailsPath)
    val deptIdList = department.getLines().toList.drop(1).map(line => {
      val arr = line.split(",")
      if (arr != null && arr(0) != null)
        arr(0).toInt
    }).map(_.toString.toInt)

    var evenCount = 1
    var oddCount = 2
    //println(deptIdList)
    for (id <- 1 to noOfEmployee) {
      // Create employee details
      val age = ageRangeStart + Random.nextInt((ageRangeEnd - ageRangeStart) + 1)
      val name = "name_" + id
      val employee = Employee(id, name, surName.get(Random.nextInt(surName.size)).getOrElse(""), age)

      employeeList = employeeList :+ employee

      // Create employee Finance details
      val ctc = ctcRangeStart + Random.nextInt((ctcRangeEnd - ctcRangeStart) + 1)
      val employeeFinance = EmployeeFinance(id, ctc, Math.round(ctc * 0.2), Math.round(ctc * 0.1), Math.round(ctc * 0.05))

      employeeFinanceList = employeeFinanceList :+ employeeFinance

      // Generate department data
      var deptId = {
        if (id % 2 == 0) { //id - 2,4,6,8,10  - all even id are for 1st 2 dept
          var dept = deptIdList(evenCount % 2) // count - 1,2,3,4,5  reminder - 0,1,0,1,0
          evenCount = evenCount + 1
          dept
        }
        else {
          // id- 1,3,5,7,9 - all odd are for last 3 dept
          var dept = deptIdList(oddCount) // count is deptId - 2,3,4,2,3,4
          oddCount = oddCount + 1
          if (oddCount > 4)
            oddCount = 2
          dept
        }
      }
      employeeDeptList = employeeDeptList :+ EmployeeDept(id, deptId)
    }

    ObjectCSV().writeCSV(employeeList, employeeDetailsPath)
    ObjectCSV().writeCSV(employeeFinanceList, employeeFinanceDetailsPath)
    ObjectCSV().writeCSV(employeeDeptList, employeeDeptDetailsPath)
  }
}
