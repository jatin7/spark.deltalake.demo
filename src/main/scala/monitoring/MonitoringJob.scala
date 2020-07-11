package com.spark.monitoring.deltalake


import monitoring.DruidIntegrationMap
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MonitoringJob {

  def main(args: Array[String]): Unit = {
    //    val kafkaServers = "3.250.149.133:9094,63.34.145.162:9094,52.30.182.144:9094"
    val kafkaServers = "localhost:9092"
    val checkpoint = "/tmp/random_spark_chk"
    val sourceTopic = "1_1_Order_Stats_Spark"
    val outputPath = "/tmp/delta/events"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val schema = new StructType()
      .add("Index", StringType)
      .add("timestamp", StringType)
      .add("Value", StringType)


    val inputStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", sourceTopic)
      .load()
    import spark.implicits._
    implicit val e = Encoders.product[Event]

    val res = inputStream
      .as[Event]
      .transform(new DruidIntegrationMap().apply(_))

    val writeQuery = res
      .writeStream
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", checkpoint)
      .start(outputPath)

    //        val writeQuery = res
    //          .writeStream
    //          .format("console")
    //          .start()

    writeQuery.awaitTermination()
  }


}
