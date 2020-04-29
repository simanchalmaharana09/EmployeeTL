package com.task.employeeTl.DataCreation

import scala.util.Random

class EmployeeFinancialDataPrep(ctcRangeStart: Int, ctcRangeEnd: Int) {
  def createEmpFinancialData(empId: Int): EmployeeFinance = {
    val ctc = ctcRangeStart + Random.nextInt((ctcRangeEnd - ctcRangeStart) + 1)
    val basic = Math.round(ctc * 0.2)
    val pf = Math.round(ctc * 0.1)
    val gratuity = Math.round(ctc * 0.05)

    val employeeFinance = EmployeeFinance(empId, ctc, basic, pf, gratuity)
    employeeFinance
  }
}
