name := "EmployeeTL"

version := "0.1"

scalaVersion := "2.12.0"

val sparkVersion = "2.4.0"

libraryDependencies += "com.typesafe" %% "config" % "1.3.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "com.gingersoftware" %% "object-csv_2.11" % "0.3"