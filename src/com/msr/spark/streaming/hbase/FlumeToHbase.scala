package com.msr.spark.streaming.hbase

import java.sql.DriverManager
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel
import org.apache.phoenix.spark._
import org.apache.spark.sql.SaveMode

object FlumeToHbase {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val columns = Seq("R", "B", "SWPD", "FREE", "BUFF", "CACH", "SI", "SO", "BI", "BO", "INS", "CS", "US", "SY", "ID", "WA", "ST")

  def main(args: Array[String]): Unit = {

    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      val con = DriverManager.getConnection("jdbc:phoenix:10.1.7.49")
      println(con)
      val rs = con.createStatement().execute("create table if not exists msrc.vmstat" +
        "(r varchar(200),b varchar(200),swpd varchar(200), free varchar(200),buff varchar(200),cach varchar(200) primary key,si varchar(200),so varchar(200),bi varchar(200)," +
        "bo varchar(200),ins varchar(200),cs varchar(200), us varchar(200),sy varchar(200),id varchar(200),wa varchar(200),st varchar(200))")
    } catch {
      case e: Exception => println(e.getMessage)
    }

    val sparkConf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val stream = FlumeUtils.createPollingStream(ssc, "localhost", 6666, StorageLevel.MEMORY_ONLY_SER_2)
    stream.foreachRDD {
      x =>

        val data = x.map(line => line.event)

        var rdd = data.map(x => new String(x.getBody.array()))

        val mytable = rdd.filter(line => !line.contains("memory")).filter(line => !line.contains("buff")).map(line => line.split("[\\s]+"))
          .map(c => (c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11), c(12), c(13), c(14), c(15), c(16), c(17)))

        mytable.take(5).foreach(println)
        mytable.saveToPhoenix("MSRC.VMSTAT", columns, zkUrl = Some("localhost"))
    }

    ssc.start
    ssc.awaitTermination
  }
}