package gjeevanm.jsonparser.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SharedSparkContext {

  lazy val sparkConf = new SparkConf()
    .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    .set("spark.executor.extraJavaOptions", "--XX:+UseG1GC")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.default.parallelism", "1")
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      .master("local")
      .appName("CSV to JSON")
      .getOrCreate()
  }
}
