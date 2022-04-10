package cn.hzhst1314.application

import cn.hzhst1314.common.TApplication
import cn.hzhst1314.controller.PerformanceController

/*
 由于recommendApplication包含数据转换，
 使用该应用获取推荐模型运行时间(除开数据转换过程)，
 集群测试使用了15min
 */
object PerformanceApplication extends TApplication{

  def main(args: Array[String]): Unit = {
    start(){
      val controller = new PerformanceController()
      controller.dispatch()
    }
  }

}
