package cn.hzhst1314.common

import cn.hzhst1314.util.EnvUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


// 应用层：应用的起始，spark环境在这一层创建，并调用控制层进行数据处理
trait TApplication {

  private val LOCAL_PATH = "local[*]"
  private val CLUSTER_PATH = "spark://10.103.105.40:7077"


  def start(master: String = CLUSTER_PATH, app: String = "Application")(op: => Unit): Unit = {
    // op代表传入的一段代码逻辑
    val sparkConf = new SparkConf().setMaster(master).setAppName(app).set("spark.executor.memory", "8g")
    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    // 放入sc
    EnvUtil.put(ss)

    try {
      op
    } catch {
      case ex: Exception => println(ex.getMessage)
    }

    ss.stop()
    EnvUtil.clear()
  }

}
