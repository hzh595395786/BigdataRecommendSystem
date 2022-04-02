package cn.hzhst1314.controller

import cn.hzhst1314.common.TController
import cn.hzhst1314.service.DataCleanService


class DataCleanController extends TController{

  private val dataCleanService = new DataCleanService()

  def dispatch(): Unit = {
    dataCleanService.yelpDataClean()
  }
}
