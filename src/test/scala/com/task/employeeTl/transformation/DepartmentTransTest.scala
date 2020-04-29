package com.task.employeeTl.transformation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.junit.Test
import org.scalatest.FunSuite

@Test
class DepartmentTransTest extends FunSuite with DataFrameSuiteBase {

  test("get Dept For Filtered Emp") {

    val deptTran: DepartmentTrans = new DepartmentTrans
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employeeDeptDf = sc.parallelize(List[(Int, Int)]
      (
        (1, 3),
        (2, 2),
        (3, 4),
        (4, 1),
        (5, 5)
      ))
      .toDF("empId", "deptId")

    val employeeDf = sc.parallelize(List[(Int)](1, 3, 5))
      .toDF("empId")

    val deptRes = deptTran.getDeptForFilteredEmp(employeeDeptDf, employeeDf)
    assert(deptRes.count() == 3)

    val expectedEmpDeptDF = sc.parallelize(List[(Int, Int)]
      (
        (1, 3),
        (3, 4),
        (5, 5)
      ))
      .toDF("empId", "deptId")

    assertDataFrameEquals(deptRes, expectedEmpDeptDF)
  }
  test("Negative Scenario : get Dept For Filtered Emp : employeeDf is empty") {

    val deptTran: DepartmentTrans = new DepartmentTrans
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employeeDeptDf = sc.parallelize(List[(Int, Int)]
      (
        (1, 3),
        (2, 2),
        (3, 4),
        (4, 1),
        (5, 5)
      ))
      .toDF("empId", "deptId")

    val employeeDf = sc.parallelize(List[(Int)]())
      .toDF("empId")

    val deptRes = deptTran.getDeptForFilteredEmp(employeeDeptDf, employeeDf)
    assert(deptRes.count() == 0)

    val list: List[(Int, Int)] = List.empty
    val expectedEmpDeptDF = sc.parallelize(list)
      .toDF("empId", "deptId")

    assertDataFrameEquals(deptRes, expectedEmpDeptDF)
  }
  test("Negative Scenario : get Dept For Filtered Emp : employeeDeptDf is empty") {

    val deptTran: DepartmentTrans = new DepartmentTrans
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val emptyList: List[(Int, Int)] = List.empty
    val employeeDeptDf = sc.parallelize(emptyList)
      .toDF("empId", "deptId")

    val employeeDf = sc.parallelize(List[(Int)](1, 3, 5))
      .toDF("empId")

    val deptRes = deptTran.getDeptForFilteredEmp(employeeDeptDf, employeeDf)
    assert(deptRes.count() == 0)

    val expectedEmpDeptDF = sc.parallelize(emptyList)
      .toDF("empId", "deptId")

    assertDataFrameEquals(deptRes, expectedEmpDeptDF)
  }

  test("find count of emp per dept") {

    val deptTran: DepartmentTrans = new DepartmentTrans
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employeeDeptDf = sc.parallelize(List[(Int, Int)]
      (
        (1, 3),
        (2, 2),
        (3, 3),
        (4, 1),
        (5, 2),
        (6, 3)
      ))
      .toDF("empId", "deptId")

    // max count of dept is 3
    val deptRes = deptTran.getDeptWithGroupedEmp(employeeDeptDf)
    assert(deptRes.count() == 1)

    // dept is 3 and no of emp is 3
    val expectedEmpDeptDF = sc.parallelize(List[(Int, Long)]((3, 3)))
      .toDF("deptId", "count")

    // deptRes will contain - empId, deptId, count
    assertDataFrameEquals(deptRes.drop("empId"), expectedEmpDeptDF)
  }
  test("Negative Scenario : find count of emp per dept : when employeeDeptDf is empty") {

    val deptTran: DepartmentTrans = new DepartmentTrans
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val emptyList: List[(Int, Int)] = List.empty
    val employeeDeptDf = sc.parallelize(emptyList)
      .toDF("empId", "deptId")

    val deptRes = deptTran.getDeptWithGroupedEmp(employeeDeptDf)
    assert(deptRes.count() == 0)

    val emptyListEx: List[(Int, Long)] = List.empty
    val expectedEmpDeptDF = sc.parallelize(emptyListEx)
      .toDF("deptId", "count")

    // deptRes will contain - empId, deptId, count
    assertDataFrameEquals(deptRes.drop("empId"), expectedEmpDeptDF)
  }

  test("get Dept Details For Max Count of employee") {

    val deptTran: DepartmentTrans = new DepartmentTrans
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val deptDf = sc.parallelize(List[(Int, String)]
      (
        (1, "IT"),
        (2, "INFRA"),
        (3, "HR"),
        (4, "ADMIN"),
        (5, "FIN")
      ))
      .toDF("deptId", "deptName")

    val deptForMaxCountEmpDF = sc.parallelize(List[(Int)]((3))).toDF("deptId").first()

    // max count of dept is 3
    val deptRes = deptTran.getDeptDetailsForMaxCount(deptDf, deptForMaxCountEmpDF)
    assert(deptRes.count() == 1)

    // dept is 3 and no of emp is 3
    val expectedDeptDF = sc.parallelize(List[(Int, String)]((3, "HR")))
      .toDF("deptId", "deptName")

    // deptRes will contain - deptId, deptName
    assertDataFrameEquals(deptRes, expectedDeptDF)
  }

  test("Negative Scenario : get Dept Details For Max Count of employee :- when deptForMaxCountEmpDF is empty ") {

    val deptTran: DepartmentTrans = new DepartmentTrans
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val deptDf = sc.parallelize(List[(Int, String)]
      (
        (1, "IT"),
        (2, "INFRA"),
        (3, "HR"),
        (4, "ADMIN"),
        (5, "FIN")
      ))
      .toDF("deptId", "deptName")

    val deptForMaxCountEmpDF: Row = Row.empty
    val resDf = deptTran.getDeptDetailsForMaxCount(deptDf, deptForMaxCountEmpDF)
    assert(resDf.count() == 0)
    /*assertThrows[ArrayIndexOutOfBoundsException] {
      deptTran.getDeptDetailsForMaxCount(deptDf, deptForMaxCountEmpDF)
    }*/
  }

}
