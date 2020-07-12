package com.spark.monitoring.deltalake

import net.bmjames.opts.{execParser, info, _}
import utils.ArgsParser._
import monitoring.DruidIntegrationMap
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scalaz.syntax.applicativePlus._
import utils.Opts

object MonitoringJob {

  def main(args: Array[String]): Unit = {

    val opts: Opts = execParser(args, this.getClass.getName, info(parseOpts <*> helper))
    val kafkaServers = opts.kafkaBootstrap.getOrElse("3.250.149.133:9094,63.34.145.162:9094,52.30.182.144:9094")
    val checkpoint = opts.checkpoint.getOrElse("/tmp/random_spark_chk2")
    val sourceTopic = opts.input.getOrElse("1_1_Order_Stats_Spark")
    val outputPath = opts.output.getOrElse("/tmp/delta/events")

    val spark = SparkSession
      .builder()
      //.master("local[*]")
      .appName("Delta monitoring JOB")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val schema = new StructType()
      .add("region", StringType)
      .add("level", IntegerType)
      .add("idAddress", StringType)
      .add("eventTime", TimestampType)


    val inputStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", sourceTopic)
      .load()

    import spark.implicits._
    implicit val e = Encoders.product[Event]

    val res = inputStream
      .select(from_json($"value".cast("string"), schema).as("data"))
      .select("data.*")
      .as[Event]

    val writeQuery = res
      .writeStream
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", checkpoint)
      .start(outputPath)

    //            val writeQuery = res
    //              .writeStream
    //              .format("console")
    //              .start()

    writeQuery.awaitTermination()
  }


}
