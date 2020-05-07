package com.gjeevanm.jsonparser

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Processor extends App {

  val sparkconf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("CsvtoJson")

  val spark = SparkSession
    .builder()
    .config(sparkconf)
    .getOrCreate()

  val inputPath = "/src/main/resources/data/data.csv"
  val outputPath = "/src/main/resources/output/"

  CsvToJson.parseCsvtoJson(spark, inputPath, outputPath)

}
