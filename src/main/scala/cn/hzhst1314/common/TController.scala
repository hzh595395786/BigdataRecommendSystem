package cn.hzhst1314.common


// 控制层:负责调度，一般会定义一个调度方法，该方法将数据处理请求调度到对应的服务
trait TController {
  def dispatch(): Unit
}