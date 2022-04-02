package cn.hzhst1314.service

import cn.hzhst1314.bean.{ItemMap, ItemRecommendList, ReviewRating, UserMap, UserRecommendList}
import cn.hzhst1314.common.TService
import cn.hzhst1314.dao.DataCleanDao
import cn.hzhst1314.util.EnvUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable


class DataCleanService extends TService{

  private val RAW_REVIEW_RATING_PATH = "hdfs://10.103.105.40:9000/dataset/yelp_academic_dataset_review.json"
  private val USER_MAP_PATH = "hdfs://10.103.105.40:9000/dataset/user_map"
  private val ITEM_MAP_PATH = "hdfs://10.103.105.40:9000/dataset/item_map"
  private val REVIEW_RATING_PATH = "hdfs://10.103.105.40:9000/dataset/rating"
  private val USER_RECOMMEND_LIST_SAVE_PATH = "hdfs://10.103.105.40:9000/dataset/user_recommend_list"
  private val ITEM_RECOMMEND_LIST_SAVE_PATH = "hdfs://10.103.105.40:9000/dataset/item_recommend_list"
  private val dataCleanDao = new DataCleanDao()

  def saveDFToJsonFile(dataCleanDF: DataFrame, path: String): Any = {
    dataCleanDao.saveDFToJsonFile(dataCleanDF, path)
  }

  def yelpDataClean(): Unit = {
    // 由于yelp数据集的userId和buinessId都是字符串，但是ml库要求为Int，所以需要进行数据清洗
    val frame = dataCleanDao.readJsonFile(RAW_REVIEW_RATING_PATH)  // 拿到原生数据，类型为dataSet
    val ss = EnvUtil.take()
    import ss.implicits._
    val ratingRDD = frame.select("user_id", "business_id", "stars").rdd.cache()
    // 得到userId和buinessId的映射表，映射方案为将所有数据去重，取其索引为其映射值
    val userIdRDD = ratingRDD.map(item => item(0)).distinct().zipWithIndex()
    val buinessIdRDD = ratingRDD.map(item => item(1)).distinct().zipWithIndex()
    val userMapDF = userIdRDD.map(item => UserMap(item._1.toString, item._2.toString.toInt)).toDF()
    val itemMapDF = userIdRDD.map(item => ItemMap(item._1.toString, item._2.toString.toInt)).toDF()
    // 保存userMap和itemMap
    saveDFToJsonFile(userMapDF, USER_MAP_PATH)
    saveDFToJsonFile(itemMapDF, ITEM_MAP_PATH)
    val userIdMap = userIdRDD.collect().toMap
    val buinessIdMap = buinessIdRDD.collect().toMap
    val ratingDF = ratingRDD.map(item => ReviewRating(
      userIdMap.get(item(0)).getOrElse().toString.toInt, buinessIdMap.get(item(1)).getOrElse().toString.toInt, item(2).toString.toFloat
    )).toDF()
    // 写入hdfs
    saveDFToJsonFile(ratingDF, REVIEW_RATING_PATH)
  }

  def userRecommendListDataClean(userRecommendListRDD: RDD[Row]): Unit = {
    // 由于在训练时，将user_id和buiness_id进行了映射，所以生成推荐列表时需要再映射回来
    val ss = EnvUtil.take()
    import ss.implicits._
    val userMapRDD = dataCleanDao.readJsonFile(USER_MAP_PATH)
      .select("user_id", "user_map_id").rdd.cache()
    val itemMapRDD = dataCleanDao.readJsonFile(ITEM_MAP_PATH)
      .select("buiness_id", "buiness_map_id").rdd.cache()
    val itemMap = itemMapRDD.map(i => Tuple2(i.get(1), i.get(0))).collect().toMap
    val userMap = userMapRDD.map(i => Tuple2(i.get(1), i.get(0))).collect().toMap
    val userRecommendListDF = userRecommendListRDD.map(item => {
      val array = item.getAs[mutable.WrappedArray[GenericRowWithSchema]](1)
      val recommendList = new Array[Map[String, Float]](array.length)
      for (i <- 0 until array.length) {
        recommendList(i) = Map[String, Float](itemMap(array(i).getInt(0)).toString -> array(i).getFloat(1))
      }
      UserRecommendList(userMap(item.getInt(0)).toString, recommendList)
    }).toDF()
    saveRecommendListToHDFS(userRecommendListDF, USER_RECOMMEND_LIST_SAVE_PATH)
  }

  def itemRecommendListDataClean(itemRecommendListRDD: RDD[Row]): Unit = {
    // 由于在训练时，将user_id和buiness_id进行了映射，所以生成推荐列表时需要再映射回来
    val ss = EnvUtil.take()
    import ss.implicits._
    val userMapRDD = dataCleanDao.readJsonFile(USER_MAP_PATH)
      .select("user_id", "user_map_id").rdd.cache()
    val itemMapRDD = dataCleanDao.readJsonFile(ITEM_MAP_PATH)
      .select("buiness_id", "buiness_map_id").rdd.cache()
    val itemMap = itemMapRDD.map(i => Tuple2(i.get(1), i.get(0))).collect().toMap
    val userMap = userMapRDD.map(i => Tuple2(i.get(1), i.get(0))).collect().toMap
    val resDF = itemRecommendListRDD.map(item => {
      val array = item.getAs[mutable.WrappedArray[GenericRowWithSchema]](1)
      val recommendList = new Array[Map[String, Float]](array.length)
      for (i <- 0 until array.length) {
        recommendList(i) = Map[String, Float](userMap(array(i).getInt(0)).toString -> array(i).getFloat(1))
      }
      ItemRecommendList(itemMap(item.getInt(0)).toString, recommendList)
    }).toDF()
    val itemRecommendListDF = resDF.toDF().withColumnRenamed("recommendList", "recommend_list")
    saveRecommendListToHDFS(itemRecommendListDF, ITEM_RECOMMEND_LIST_SAVE_PATH)
  }

  def saveRecommendListToHDFS(userRecommendList: DataFrame, path: String): Unit = {
    // 将推荐列表以json文件保存到hdfs
    dataCleanDao.saveDFToJsonFile(userRecommendList, path)
  }

  def saveRecommendListToMySql(userRecommendListDF: DataFrame, table_name: String): Unit = {
    // 将推荐列表保存到mysql
    import org.apache.spark.sql.functions._
    val colNames = userRecommendListDF.columns
    val mysqlDF = userRecommendListDF
      .withColumn(colNames(0), col(colNames(0)).cast(org.apache.spark.sql.types.StringType))
      .withColumn(colNames(1), col(colNames(1)).cast(org.apache.spark.sql.types.StringType))
    dataCleanDao.saveDFToMySql(mysqlDF, table_name)
  }
}
