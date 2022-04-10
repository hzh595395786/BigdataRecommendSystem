package cn.hzhst1314.service

import cn.hzhst1314.bean.ReviewRating
import cn.hzhst1314.common.TService
import cn.hzhst1314.dao.PerformanceDao
import cn.hzhst1314.util.{ALSUtil, EnvUtil};

class PerformanceService extends TService{

  private val REVIEW_RATING_PATH = "hdfs://10.103.105.40:9000/dataset/rating"
  private val performanceDao = new PerformanceDao()

  def performanceTest(): Unit = {
    val frame = performanceDao.readJsonFile(REVIEW_RATING_PATH)
    val ss = EnvUtil.take()
    import ss.implicits._
    val ratingDS = frame.select( "user_id", "business_id", "rating")
    val resDS = ratingDS.map(item => ReviewRating(
      item.get(0).toString.toInt, item.get(1).toString.toInt, item.get(2).toString.toFloat
    ))
    // 训练模型
    val model = ALSUtil.train(resDS)
    val itemRecommendList = model.recommendForAllItems(10)
    val userRecommendList = model.recommendForAllUsers(10)
  }

}
