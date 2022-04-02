package cn.hzhst1314.application

import cn.hzhst1314.common.TApplication
import cn.hzhst1314.controller.RecommendController


// 推荐app，主要用于模型训练和推荐列表生成
object RecommendApplication extends TApplication{

  def main(args: Array[String]): Unit = {
    start(){
      val controller = new RecommendController()
      controller.dispatch()
    }
  }

}
