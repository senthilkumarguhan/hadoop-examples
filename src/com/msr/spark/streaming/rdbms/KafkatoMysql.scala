package com.msr.spark.streaming.rdbms

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext

object KafkatoMysql {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val (zkQuorum, group, topics, numThreads) = ("localhost:2181", "kelly", "trainee", "2")
  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName).setMaster("local[4]")
    .set("spark.cassandra.connection.host", "10.1.7.49")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(2))
  val sqlContext = new SQLContext(ssc.sparkContext)

  import sqlContext.implicits._

  case class vmstat(r: String, b: String, swpd: String,
                    free: String, buff: String, cache: String,
                    si: String, so: String, bi: String,
                    bo: String, ins: String, cs: String,
                    us: String, sy: String, id: String,
                    wa: String, st: String)

  def main(args: Array[String]): Unit = {

    val prop = new java.util.Properties
    prop.put("user", "root")
    prop.put("password", "hadoop")
    prop.put("driverClass", "com.mysql.jdbc.Driver")

    val uri = "jdbc:mysql://10.1.7.49:3306/test"
    val table = "vmstat"

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    lines.foreachRDD { rdd =>

      val mytable = rdd.filter(line => !line.contains("memory")).filter(line => !line.contains("buff")).map(line => line.split("[\\s]+"))
        .map(c => vmstat(c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12), c(13), c(14), c(15), c(16), c(17))).toDF

      mytable.take(5).foreach(println)

      mytable.write.mode(SaveMode.Append).jdbc(uri, table, prop)
    }
    ssc.start
    ssc.awaitTermination
  }
}