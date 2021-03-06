package com.msr.spark.sql.hbase

import org.apache.log4j.Level
import org.apache.log4j.Logger
import java.sql.DriverManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.databricks.spark.avro._
import org.apache.spark.sql.Row
import org.apache.phoenix.spark._

object AvroToHbase {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  case class employees(emp_no: String, birth_date: String, first_name: String, last_name: String, gender: String, hire_date: String)

  val sparkConf = new SparkConf().setAppName(this.getClass.getName)
    .setMaster("local")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def main(args: Array[String]): Unit = {

    try {
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      val con = DriverManager.getConnection("jdbc:phoenix:10.1.7.49")
      println(con)
      val rs = con.createStatement().execute("create table if not exists employees("
        + "emp_no varchar(200) primary key"
        + ",birth_date varchar(200),first_name varchar(200) ,"
        + "last_name varchar(200),gender varchar(200),hire_date varchar(200))")
    } catch {
      case e: Exception => println(e.getMessage)
    }

    
    val file = "file:/home/user/Documents/dataset/employees.avro"
    
    val empdf = sqlContext.read.avro(file)
    
    val temptable = empdf.map {
                        
                 case Row(emp_no, birth_date, first_name, last_name, gender, hire_date) 
                 
                 =>
                
                employees(emp_no.toString, 
                birth_date.asInstanceOf[String], 
                first_name.asInstanceOf[String], 
                last_name.asInstanceOf[String], 
                gender.asInstanceOf[String], 
                hire_date.asInstanceOf[String])
        }
    
     temptable.saveToPhoenix("employees", Seq("EMP_NO", "BIRTH_DATE", "FIRST_NAME", "LAST_NAME", "GENDER", "HIRE_DATE"), zkUrl = Some("localhost"))
    println("success")
  }
}