package gjeevanm.jsonparser

import com.gjeevanm.jsonparser.CsvToJson.parseCsvtoJson
import gjeevanm.jsonparser.utils.SharedSparkContext
import org.scalatest.FlatSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField}

class CsvToJsonTest extends FlatSpec with SharedSparkContext {

  "parseCsvtoJson" should "return proper JSON" in {
    val inputPath = "/src/test/resources/data/data.csv"
    val outputPath = "/src/test/resources/output/"

    parseCsvtoJson(spark, inputPath, outputPath)

    val df = spark.read.format("json")
      .load(System.getProperty("user.dir") + outputPath)

    df.show()
  }
}
