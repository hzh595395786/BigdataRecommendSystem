package cn.hzhst1314.service

import cn.hzhst1314.bean.ReviewRating
import cn.hzhst1314.common.TService
import cn.hzhst1314.dao.RecommendDao
import cn.hzhst1314.util.{ALSUtil, EnvUtil}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame

class RecommendService extends TService{

  private val REVIEW_RATING_PATH = "hdfs://10.103.105.40:9000/dataset/rating"
  private val recommendDao = new RecommendDao()


  def trainALSModel(): ALSModel = {
    // 从hdfs中读取数据集
    val frame = recommendDao.readJsonFile(REVIEW_RATING_PATH)
    // 数据清洗，只保留三列数据
    val ss = EnvUtil.take()
    import ss.implicits._
    val ratingDS = frame.select( "user_id", "business_id", "rating")
    val resDS = ratingDS.map(item => ReviewRating(
      item.get(0).toString.toInt, item.get(1).toString.toInt, item.get(2).toString.toFloat
    ))
    // 训练模型
    ALSUtil.train(resDS)
  }

}
