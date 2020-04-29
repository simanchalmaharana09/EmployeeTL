package com.task.employeeTl.DataCreation

import scala.util.Random

class EmployeePersonalDataPre(ageRangeStart: Int, ageRangeEnd: Int, surNameMap: Map[Int, String]) {
  def createEmpPersonalData(empId: Int): Employee = {
    val age = ageRangeStart + Random.nextInt((ageRangeEnd - ageRangeStart) + 1)
    val name = "name_" + empId
    val employee = Employee(empId, name, surNameMap.get(Random.nextInt(surNameMap.size)).getOrElse("Afzal"), age)
    employee
  }
}
