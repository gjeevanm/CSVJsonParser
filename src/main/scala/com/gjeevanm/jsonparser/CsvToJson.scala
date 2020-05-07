package com.gjeevanm.jsonparser

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession}

object CsvToJson {

  def parseCsvtoJson(spark: SparkSession, inputPath: String, outputPath: String): Unit = {

    val df = spark.read.format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(System.getProperty("user.dir") + inputPath)

    import spark.implicits._

    val transformedDf = df
      .withColumn("Person_id",col("Person Id").cast(IntegerType))
      .withColumn("datetime", to_date(unix_timestamp($"Floor Access DateTime", "MM/dd/yy HH:mm").cast("timestamp")))
      .withColumn("floor_level",col("Floor Level").cast(IntegerType))
      .withColumn("Building",col("building"))

        .select("person_id","datetime","floor_level","building")

    transformedDf.write.format("json")
      .option("timestampFormat", "yyyy-MM-dd HH:mm")
      .mode(SaveMode.Overwrite)
      .save(System.getProperty("user.dir") + outputPath)

  }


}
