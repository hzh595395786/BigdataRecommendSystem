package cn.hzhst1314.controller

import cn.hzhst1314.common.TController
import cn.hzhst1314.service.PerformanceService

class PerformanceController extends TController{

  private val service = new PerformanceService()

  def dispatch(): Unit = {
    service.performanceTest()
  }
}
