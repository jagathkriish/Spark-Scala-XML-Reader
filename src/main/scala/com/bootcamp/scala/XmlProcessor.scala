package com.bootcamp.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import com.databricks.spark.xml

object XmlProcessor {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("XmlProcessor").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc);
    loadXmlData(sqlContext)
    println("Hello")
  }

  def loadXmlData(sqlContext: SQLContext) = {
    var df: DataFrame = null
    var newDf: DataFrame = null

    import sqlContext.implicits._

    df = sqlContext.read.format("xml").option("rowTag", "book")
      .load("datafiles/books.xml")
    //df.printSchema()
    df.createOrReplaceTempView("books")
    sqlContext.sql(""" select * from books where _id = 'bk111' """).show()
    
    var df2: DataFrame = null;
    df2 = sqlContext.read.format("xml").option("rowTag", "publishEvent")
      .load("datafiles/sample.xml")
    df2.printSchema()
    df2.createOrReplaceTempView("sample")
    sqlContext.sql(""" select EventEnvelope.Body.EventNotificationDetails.CIF from sample where EventEnvelope.Header.siid = 'a4a23e88-d388-4f2d-963d-66a65fcc5030' """).show()
  }

}