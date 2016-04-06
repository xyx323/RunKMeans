import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/***test ****/
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
/*******/
/**
  * Created by xyx on 3/30/16.
  */

object StreamingPredict{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("K-means").setMaster("local"))
    if(modelWasExsited)
      runStreamingPredict()
    else
      buildModel(sc)


    //runStreamingPredict()


    //buildLatestModel(rawData,sc)

  }
  def modelWasExsited():Boolean ={
    return false
  }
  def loadLocalModel(sc:SparkContext): KMeansModel ={
    return KMeansModel.load(sc,"/home/xyx/Desktop/model")
  }
  def runStreamingPredict(): Unit ={

  }
  def buildModel(sc:SparkContext): Unit ={
    val rawData = readLocalFile(sc)

    buildKmeansModel(sc,rawData)

  }
  def readLocalFile(sc:SparkContext): RDD[String] ={
    return sc.textFile("file:///home/xyx/Desktop/kddcup.data_10_percent_corrected")
  }
  def buildKmeansModel(sc:SparkContext,rawData: RDD[String]): Unit ={
    val parseFunction = buildCategoricalAndLabelFunction(rawData)
    val data = rawData.map(parseFunction).values
    //
    val test = rawData.map(parseFunction)
    //
    val normalizedData = data.map(buildNormalizationFunction(data)).cache()

    val kmeans = new KMeans()
    kmeans.setK(150)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)
    val score = normalizedData.map(datum => distToCentroid(datum, model)).mean()
    print(score)

    /**test Evaluation Metrics**/
    Precisionbythreshold(test,model)

    normalizedData.unpersist()
    model.save(sc,"/home/xyx/Desktop/model/")
  }
  def buildCategoricalAndLabelFunction(rawData: RDD[String]): (String => (String,Vector)) = {
    val splitData = rawData.map(_.split(','))
    val protocols = splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val services = splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = splitData.map(_(3)).distinct().collect().zipWithIndex.toMap
    (line: String) => {
      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)
      val vector = buffer.map(_.toDouble)

      val newProtocolFeatures = new Array[Double](protocols.size)
      newProtocolFeatures(protocols(protocol)) = 1.0
      val newServiceFeatures = new Array[Double](services.size)
      newServiceFeatures(services(service)) = 1.0
      val newTcpStateFeatures = new Array[Double](tcpStates.size)
      newTcpStateFeatures(tcpStates(tcpState)) = 1.0

      vector.insertAll(1, newTcpStateFeatures)
      vector.insertAll(1, newServiceFeatures)
      vector.insertAll(1, newProtocolFeatures)

      (label, Vectors.dense(vector.toArray))
    }
  }

  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce(
      (a, b) => a.zip(b).map(t => t._1 + t._2))
    val sumSquares = dataAsArray.aggregate(
      new Array[Double](numCols)
    )(
      (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2),
      (a, b) => a.zip(b).map(t => t._1 + t._2)
    )
    val stdevs = sumSquares.zip(sums).map {
      case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    val means = sums.map(_ / n)

    (datum: Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0)  (value - mean) else  (value - mean) / stdev
      )
      Vectors.dense(normalizedArray)
    }
  }


  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  /*********************** test Evaluation Metrics**********************************************/
  def Precisionbythreshold(test:RDD[(String,Vector)],model:KMeansModel): Unit ={
    // Compute raw scores on the test set
    val predictionAndLabels = test.map { case LabeledPoint(label ,features) =>
      val prediction = model.predict(features).toDouble
      (prediction, label)
    }

    // Instantiate metrics object
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }


  }


  /************************test Evaluation Metrics********************************************/
}