package com.task.employeeTl.DataCreation

import com.gingersoftware.csv.ObjectCSV

object EmployeeDataCreation {

  def main(args: Array[String]): Unit = {
    // Read properties
    val ENV = "dev"
    val properties: EnvProperties = new Utils().populateProperties(ENV)

    var employeeList = List[Employee]()
    var employeeFinanceList = List[EmployeeFinance]()
    var employeeDeptList = List[EmployeeDept]()

    val empPersonalDataPrep = new EmployeePersonalDataPre(properties.ageRangeStart, properties.ageRangeEnd, properties.surNameMap)
    val empFinancialDataPrep = new EmployeeFinancialDataPrep(properties.ctcRangeStart, properties.ctcRangeEnd)

    var deptIdList: List[Int] = new Utils().prepareDeptList(properties.departmentDetailsPath)
    val empDeptDataPrep = new EmployeeDepartmentDataPrep(deptIdList)

    var evenCount: Int = 1
    var oddCount: Int = 2

    for (empId <- 1 to properties.noOfEmployee) {
      // Create employee details
      val employee = empPersonalDataPrep.createEmpPersonalData(empId)
      employeeList = employeeList :+ employee

      // Create employee Finance details
      val employeeFinance = empFinancialDataPrep.createEmpFinancialData(empId)
      employeeFinanceList = employeeFinanceList :+ employeeFinance

      // Generate department data
      val (deptId: Int, evenCountTemp: Int, oddCountTemp: Int) = empDeptDataPrep.createEmpDeptData(empId, evenCount, oddCount)
      employeeDeptList = employeeDeptList :+ EmployeeDept(empId, deptId)

      evenCount = evenCountTemp
      oddCount = oddCountTemp
    }
    // writing all data to output path
    ObjectCSV().writeCSV(employeeList, properties.employeeDetailsPath)
    ObjectCSV().writeCSV(employeeFinanceList, properties.employeeFinanceDetailsPath)
    ObjectCSV().writeCSV(employeeDeptList, properties.employeeDeptDetailsPath)

    println("Data creation completed")
  }
}