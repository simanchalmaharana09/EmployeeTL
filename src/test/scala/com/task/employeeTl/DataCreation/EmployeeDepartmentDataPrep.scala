package com.task.employeeTl.DataCreation

class EmployeeDepartmentDataPrep(deptIdList: List[Int]) {

  def createEmpDeptData(empId: Int, evenCount: Int, oddCount: Int): (Int, Int, Int) = {
    // Generate department data
    /*
    1,IT
    2,INFRA
    3,HR
    4,ADMIN
    5,FIN
    */
    var evenCountTemp = evenCount
    var oddCountTemp = oddCount
    var outDeptId = {
      if (empId % 2 == 0) { //id - 2,4,6,8,10  - all even emp-id are for 1st 2 dept only
        var deptId = deptIdList(evenCountTemp % 2) // count - 1,2,3,4,5  reminder - 0,1,0,1,0. so dept seq is IT, INFRA,IT,INFRA,IT
        evenCountTemp = evenCountTemp + 1
        deptId
      }
      else {
        // id- 1,3,5,7,9 - all odd are for last 3 dept
        var deptId = deptIdList(oddCount) // count is deptId - 2,3,4,2,3,4
        oddCountTemp = oddCountTemp + 1
        if (oddCountTemp > 4) { // resetting count for starting from 3rd Dept ( HR )
          oddCountTemp = 2 // counter set to 3 : HR ( for array it's 2 )
        }
        deptId
      }
    }
    (outDeptId, evenCountTemp, oddCountTemp)
  }
}
