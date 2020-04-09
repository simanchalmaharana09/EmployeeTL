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

    val employeeFin = sc.parallelize(List[(Int, Double, Double, Double, Double)]
      (
        (1, 98127.0, 19625.0, 9813.0, 4906.0),
        (2, 15667.0, 3133.0, 1567.0, 783.0),
        (3, 59641.0, 11928.0, 5964.0, 2982.0),
        (4, 32731.0, 2546.0, 1273.0, 637.0),
        (5, 20000.0, 1200.0, 800.0, 900.0),
        (6, 25000.0, 1200.0, 800.0, 1200.0)
      ))
      .toDF("empId", "ctc", "basic", "pf", "gratuity")
    val empRes = EmployeeTrans.getEmployeeAge35Plus(employee)
    assert(empRes.count() == 2)
    // asserting for count of employee (whose age <=35) == 0
    assert((empRes.filter(empRes("age") <= 35).count() == 0))
  }
  test("Filter employee ctc >30000 and gratuity <800") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employee = sc.parallelize(List[(Int, String, String, Int)]
      ((1, "abc", "xyz", 25), (2, "abc1", "xyz1", 35), (3, "abc2", "xyz2", 42), (4, "abc3", "xyz3", 45)))
      .toDF("empId", "firstName", "lastName", "age")

    val employeeFin = sc.parallelize(List[(Int, Double, Double, Double, Double)]
      (
        (1, 98127.0, 19625.0, 9813.0, 4906.0),
        (2, 15667.0, 3133.0, 1567.0, 783.0),
        (3, 59641.0, 11928.0, 5964.0, 2982.0),
        (4, 32731.0, 2546.0, 1273.0, 637.0),
        (5, 20000.0, 1200.0, 800.0, 900.0),
        (6, 25000.0, 1200.0, 800.0, 1200.0)
      ))
      .toDF("empId", "ctc", "basic", "pf", "gratuity")
    val empRes = EmployeeTrans.getEmployeeFinanceDF(employeeFin)
    assert(empRes.count() == 4)
    // asserting for count of employee (whose ctc <=30000 and gratuity < 800) == 0
    assert((empRes.filter(empRes("ctc") <= 30000 && empRes("gratuity") >= 800).count() == 0))
  }
  test("getEmpDfAge40PlusCtc30kPlus") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employee = sc.parallelize(List[(Int, String, String, Int)]
      ((1, "abc", "xyz", 25), (2, "abc1", "xyz1", 35), (3, "abc2", "xyz2", 42), (4, "abc3", "xyz3", 45)))
      .toDF("empId", "firstName", "lastName", "age")

    val employeeFin = sc.parallelize(List[(Int, Double, Double, Double, Double)]
      (
        (1, 98127.0, 19625.0, 9813.0, 4906.0),
        (2, 15667.0, 3133.0, 1567.0, 783.0),
        (3, 59641.0, 11928.0, 5964.0, 2982.0),
        (4, 32731.0, 2546.0, 1273.0, 637.0),
        (5, 20000.0, 1200.0, 800.0, 900.0),
        (6, 25000.0, 1200.0, 800.0, 1200.0)
      ))
      .toDF("empId", "ctc", "basic", "pf", "gratuity")
    val resDf = EmployeeTrans.getEmpDfAge40PlusCtc30kPlus(employee, employeeFin)
    assert(resDf.count() == 2)

    // Only emp id - 3 & 4 satisfy the condition. so in result only  3 and 4 empId is present
    val expectedEmpIdDF = sc.parallelize(List[Int](3, 4)).toDF("empId")
    assertDataFrameEquals(resDf.select(employee("empId")), expectedEmpIdDF)
  }
  test("getEmpAge > 35 and Grat < 800") {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val employee = sc.parallelize(List[(Int, String, String, Int)]
      ((1, "abc", "xyz", 25), (2, "abc1", "xyz1", 35), (3, "abc2", "xyz2", 42), (4, "abc3", "xyz3", 45)))
      .toDF("empId", "firstName", "lastName", "age")

    val employeeFin = sc.parallelize(List[(Int, Double, Double, Double, Double)]
      (
        (1, 98127.0, 19625.0, 9813.0, 4906.0),
        (2, 15667.0, 3133.0, 1567.0, 783.0),
        (3, 59641.0, 11928.0, 5964.0, 2982.0),
        (4, 32731.0, 2546.0, 1273.0, 637.0),
        (5, 20000.0, 1200.0, 800.0, 900.0),
        (6, 25000.0, 1200.0, 800.0, 1200.0)
      ))
      .toDF("empId", "ctc", "basic", "pf", "gratuity")

    val empAgeGt35 = EmployeeTrans.getEmployeeAge35Plus(employee) // age > 45 for empId 3 and 4
    val empRes = EmployeeTrans.getEmpAge35PlusGrat800Less(empAgeGt35, employeeFin) // grats < 800 for empid 4
    assert(empRes.count() == 1)
    // asserting for count of employee (whose age <=35) == 0
    assert((empRes.filter(empRes("age") <= 35).count() == 0))

    // asserting for count of employee (whose gratuity >=800) == 0
    assert((empRes.filter(empRes("gratuity") >= 800).count() == 0))

    // asserting for employeeId dataframe
    val expectedEmpIdDF = sc.parallelize(List[Int](4)).toDF("empId")
    assertDataFrameEquals(empRes.select(employee("empId")), expectedEmpIdDF)
  }

  // Need to write for negative scenario also
}
