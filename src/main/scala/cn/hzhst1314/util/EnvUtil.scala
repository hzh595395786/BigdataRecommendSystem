package cn.hzhst1314.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object EnvUtil {

  private val scLocal = new ThreadLocal[SparkSession]()

  def put(ss: SparkSession): Unit = {
    scLocal.set(ss)
  }

  def take(): SparkSession = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }

}
