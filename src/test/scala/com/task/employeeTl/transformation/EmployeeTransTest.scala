package com.task.employeeTl.transformation

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.junit.Test
import org.scalatest.FunSuite

@Test
class EmployeeTransTest extends FunSuite with DataFrameSuiteBase {

  test("employee age > 35 test") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employee = sc.parallelize(List[(Int, String, String, Int)]
      ((1, "abc", "xyz", 25), (2, "abc1", "xyz1", 35), (3, "abc2", "xyz2", 42), (4, "abc3", "xyz3", 45)))
      .toDF("empId", "firstName", "lastName", "age")

    val empRes = EmployeeTrans.filterEmployeeByAge(employee)
    assert(empRes.count() == 2)
    // asserting for count of employee (whose age <=35) == 0
    assert((empRes.filter(empRes("age") <= 35).count() == 0))
  }
  test("Filter employee ctc >30000 and gratuity <800") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employeeFin = sc.parallelize(List[(Int, Double, Double, Double, Double)]
      (
        (1, 98127.0, 19625.0, 9813.0, 4906.0), //1
        (2, 15667.0, 3133.0, 1567.0, 783.0), //2
        (3, 59641.0, 11928.0, 5964.0, 2982.0), //3
        (4, 32731.0, 2546.0, 1273.0, 637.0), //4
        (5, 20000.0, 1200.0, 800.0, 900.0),
        (6, 25000.0, 1200.0, 800.0, 1200.0)
      ))
      .toDF("empId", "ctc", "basic", "pf", "gratuity")

    val empFinRes = EmployeeTrans.filterEmpFinanceByCtcAndGrats(employeeFin)
    assert(empFinRes.count() == 4)
    // asserting for count of employee (whose ctc <=30000 and gratuity < 800) == 0
    assert((empFinRes.filter(empFinRes("ctc") <= 30000 && empFinRes("gratuity") >= 800).count() == 0))
  }
  test("filter for Age > 40 and Ctc > 30k") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employeeWithFinDF = sc.parallelize(List[(Int, String, String, Int, Double)]
      (
        (1, "abc", "xyz", 25, 32000.0),
        (2, "abc1", "xyz1", 35, 25000.0),
        (3, "abc2", "xyz2", 42, 35000.0),
        (4, "abc3", "xyz3", 45, 20000.0)))
      .toDF("empId", "firstName", "lastName", "age", "ctc")

    val resDf = EmployeeTrans.filterForAge40PlusCtc30kPlus(employeeWithFinDF)
    assert(resDf.count() == 1)

    // Only emp id - 3 only satisfy the condition. so in result only  empid - 3 record  is present
    val expectedEmpDF = sc.parallelize(List[(Int, String, String, Int, Double)]
      ((3, "abc2", "xyz2", 42, 35000.0)))
      .toDF("empId", "firstName", "lastName", "age", "ctc")

    assertDataFrameEquals(resDf, expectedEmpDF)
  }
  test("filterEmpGrat < 800") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employeeWithFinDF = sc.parallelize(List[(Int, String, String, Int, Double, Double)]
      (
        (1, "abc", "xyz", 25, 32000.0, 900.0),
        (2, "abc1", "xyz1", 35, 25000.0, 805.0),
        (3, "abc2", "xyz2", 42, 35000.0, 505.0),
        (4, "abc3", "xyz3", 45, 20000.0, 200)))
      .toDF("empId", "firstName", "lastName", "age", "ctc", "gratuity")

    val empRes = EmployeeTrans.filterEmpForGrat800Less(employeeWithFinDF) // grats < 800 for empid 4
    assert(empRes.count() == 2)

    // asserting for count of employee (whose gratuity >=800) == 0
    assert((empRes.filter(empRes("gratuity") >= 800).count() == 0))

    // asserting for employeeId dataframe
    val expectedEmpIdDF = sc.parallelize(List[(Int, String, String, Int, Double, Double)]
      (
        (3, "abc2", "xyz2", 42, 35000.0, 505.0),
        (4, "abc3", "xyz3", 45, 20000.0, 200)))
      .toDF("empId", "firstName", "lastName", "age", "ctc", "gratuity")

    assertDataFrameEquals(empRes, expectedEmpIdDF)
  }

  // TODO - Negative scenario
}
