package com.example
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object SparkConnectionUtil {

  private val appName = "V2 Maestros"
  //Pointer / URL to the Spark instance - embedded
  private val sparkMaster = "local[2]"

  private var spContext :SparkContext = null
  private var sparkSession:SparkSession = null
  private val tempDir = "file:///c:/temp/spark-warehouse"


  private def bootstrap(): Unit ={

    if (spContext == null && sparkSession == null) { //Setup Spark configuration
      val conf = new SparkConf().setAppName(appName).setMaster(sparkMaster)
      System.setProperty("hadoop.home.dir", "C:\\Users\\I320209\\Documents\\WinUtils\\")
      spContext = new SparkContext(conf)
      sparkSession=SparkSession.builder.appName(appName).master(sparkMaster).config("spark.sql.warehouse.dir", tempDir).getOrCreate()
    }
  }

  def getSparkSession(): SparkSession ={
     if(sparkSession == null){
       bootstrap()
     }
    return sparkSession
  }

  def getSparkContext(): SparkContext ={
    if (spContext == null){
      bootstrap()
    }
    return spContext
  }
}
