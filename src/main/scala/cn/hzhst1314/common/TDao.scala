package cn.hzhst1314.common

import cn.hzhst1314.util.EnvUtil
import org.apache.spark.sql.DataFrame


// 持久层:负责数据相关，本项目定义读取文件和保存文件的方法
trait TDao {

  private val DEFAULT_SAVE_PATH = "hdfs://10.103.105.40:9000/dataset/default_save"
  private val MYSQL_URL = "jdbc:mysql://localhost:3306/recommend?serverTimezone=GMT%2B8"
  private val MYSQL_USERNAME = "root"
  private val MYSQL_PWD = "mysql"

  def readJsonFile(path: String): DataFrame = {
    val spark = EnvUtil.take()
    import spark.implicits._
    spark.read.json(path)
  }

  def saveDFToJsonFile(dataFrame: DataFrame, path: String = DEFAULT_SAVE_PATH): Unit= {
    dataFrame.write.mode("overwrite").json(path)
  }

  def saveDFToMySql(mysqlDF: DataFrame, table_name: String): Unit = {
    val prop = new java.util.Properties
    prop.setProperty("user", MYSQL_USERNAME)
    prop.setProperty("password", MYSQL_PWD)
    mysqlDF.write.mode("overwrite").jdbc(MYSQL_URL, table_name, prop)
  }

}