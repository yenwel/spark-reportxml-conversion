name := "ReportsConversion"
version := "1.0"
scalaVersion := "2.11.8"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.10
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"
libraryDependencies += "com.twitter" % "util-core_2.11" % "6.41.0"
