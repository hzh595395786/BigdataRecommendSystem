package cn.hzhst1314.util


import cn.hzhst1314.bean.ReviewRating
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.Dataset



object ALSUtil {

  private val MODEL_SAVE_PATH = "hdfs://10.103.105.40:9000/dataset/model"

  def train(reviewRatingDS: Dataset[ReviewRating]): ALSModel = {
    val spark = EnvUtil.take()
    import spark.implicits._
    // 将数据集分为训练集和测试集
    val splits = reviewRatingDS.randomSplit(Array(0.8, 0.2))
    val trainingDS = splits(0)
    val testingDS = splits(1)
    // 真正的训练
    trainALSModel( trainingDS, testingDS)
  }

  def trainALSModel(trainDS: Dataset[ReviewRating], testDS: Dataset[ReviewRating]): ALSModel ={
    // 遍历数组中定义的参数取值
    val result = for( rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01) )
      yield {
        val als = new ALS()
          .setRank(rank) // 设置k的大小
          .setMaxIter(10) // 迭代次数
          .setRegParam(lambda)  // 学习率
          .setUserCol("user_id")
          .setItemCol("business_id")
          .setRatingCol("rating")
        val model = als.fit(trainDS)  // 训练
        val rmse = getRMSE( model, testDS )
        ( rank, lambda, rmse, model )
      }
    // 按照rmse排序并输出最优模型
    val model = result.minBy(_._3)._4
    saveModel(model)  // 保存模型
    model
  }

  def saveModel(model: ALSModel): Unit = {
    try {
      model.save(MODEL_SAVE_PATH)
    } catch {
      case e: Exception => println("模型保存出错：" + e.getMessage)
    }
  }

  def getTrainedModel(): ALSModel = throw new Exception("模型加载出错"){
    ALS.load(MODEL_SAVE_PATH)
  }

  def getRMSE(model: ALSModel, testDS: Dataset[ReviewRating]): Double = {
    // 通过测试集计算得到rmse
    val predictRating = model.transform(testDS)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictRating)
    rmse
  }
}
