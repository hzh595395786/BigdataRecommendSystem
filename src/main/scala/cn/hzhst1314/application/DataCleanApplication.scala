package cn.hzhst1314.application

import cn.hzhst1314.common.TApplication
import cn.hzhst1314.controller.DataCleanController


// 数据清洗app，主要用于将数据进行清洗
object DataCleanApplication extends TApplication{

  def main(args: Array[String]): Unit = {
    start(){
      val controller = new DataCleanController()
      controller.dispatch()
    }
  }

}
