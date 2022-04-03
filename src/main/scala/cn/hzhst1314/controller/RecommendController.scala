package cn.hzhst1314.controller

import cn.hzhst1314.common.TController
import cn.hzhst1314.service.{DataCleanService, RecommendService}
import cn.hzhst1314.util.ALSUtil


class RecommendController extends TController{

  private val recommendService = new RecommendService()
  private val dataCleanService = new DataCleanService()

  def dispatch(): Unit = {
    // 首先从读取是否有训练好的模型
    var model = try {
      ALSUtil.getTrainedModel()
    } catch {
      case exception: Exception => println(exception.getMessage)
        null
    }
    if (model == null) {
      model = recommendService.trainALSModel()
    }
    val itemRecommendList = model.recommendForAllItems(10)
    dataCleanService.itemRecommendListDataClean(itemRecommendList.rdd)
    val userRecommendList = model.recommendForAllUsers(10)
    dataCleanService.userRecommendListDataClean(userRecommendList.rdd)
  }
}
